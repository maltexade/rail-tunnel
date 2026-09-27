package handlers

import (
	"crypto/rand"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"rail-tunnel/managers"
	"rail-tunnel/models"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type TunnelHandlers struct {
	connectionManager *managers.ConnectionManager
	upgrader          websocket.Upgrader
	pendingRequests   map[string]*PendingRequest
	pendingMu         sync.RWMutex

	// writeMu guards all writes to the active WebSocket.
	// gorilla/websocket allows only ONE concurrent writer per conn.
	writeMu sync.Mutex
}

type PendingRequest struct {
	ResponseChan chan *models.Message
	Timeout      time.Time
}

func NewTunnelHandlers(cm *managers.ConnectionManager) *TunnelHandlers {
	return &TunnelHandlers{
		connectionManager: cm,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		pendingRequests: make(map[string]*PendingRequest),
	}
}

// safeWriteJSON serializes all writes to the WebSocket.
// Every place that writes to connection.Conn must go through this.
func (h *TunnelHandlers) safeWriteJSON(conn *websocket.Conn, v interface{}) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	return conn.WriteJSON(v)
}

// HandleWebSocket handles WS /ws/connect - Auto-configure tunnel
func (h *TunnelHandlers) HandleWebSocket(c *gin.Context) {
	localPort := c.Query("port")
	if localPort == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing 'port' parameter"})
		return
	}

	serverURL := getServerURL(c.Request)
	req := models.ConnectRequest{
		LocalPort: parsePort(localPort),
		LocalURL:  fmt.Sprintf("http://localhost:%s", localPort),
	}
	connection := h.connectionManager.Connect(req, serverURL)
	log.Printf("Auto-configured tunnel: %s -> %s", connection.PublicURL, connection.LocalURL)

	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	connection.Conn = conn
	h.connectionManager.SetConnected(true)
	log.Printf("WebSocket connected: %s -> %s", connection.PublicURL, connection.LocalURL)

	// Only ONE reader goroutine allowed per conn — this is that goroutine.
	for {
		var msg models.Message
		if err := conn.ReadJSON(&msg); err != nil {
			log.Printf("WebSocket read error: %v", err)
			break
		}
		h.handleWebSocketMessage(&msg)
	}

	h.connectionManager.SetConnected(false)
	log.Printf("WebSocket disconnected")
}

func (h *TunnelHandlers) HandleTunnelTraffic(c *gin.Context) {
	connection, connected := h.connectionManager.GetConnection()
	if !connected || connection.Conn == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "Tunnel not available",
			"message": "No tunnel connected. Start your CLI client first.",
		})
		return
	}

	requestPath := c.Request.URL.Path
	if c.Request.URL.RawQuery != "" {
		requestPath += "?" + c.Request.URL.RawQuery
	}

	h.forwardRequestToTunnel(c, connection, requestPath)
}

func (h *TunnelHandlers) handleWebSocketMessage(msg *models.Message) {
	switch msg.Type {
	case "ping":
		log.Printf("Received ping from tunnel")
	case "pong":
		log.Printf("Received pong from tunnel")
	case "http_response":
		h.handleResponse(msg)
	default:
		log.Printf("Unknown message type: %s", msg.Type)
	}
}

func (h *TunnelHandlers) forwardRequestToTunnel(c *gin.Context, connection *models.TunnelConnection, requestPath string) {
	requestID := uuid.New().String()

	// Read request body safely. Using ContentLength directly is unsafe
	// when the header is missing (chunked bodies) — read up to a limit.
	const maxBody = 10 << 20 // 10 MB
	var body interface{}
	if c.Request.Body != nil {
		raw, err := readBody(c.Request.Body, maxBody)
		if err != nil {
			log.Printf("Failed to read request body: %v", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "Could not read body"})
			return
		}
		if len(raw) > 0 {
			body = string(raw)
		}
	}

	msg := models.Message{
		Type:      "http_request",
		RequestID: requestID,
		Method:    c.Request.Method,
		URL:       requestPath,
		Headers:   c.Request.Header,
		Body:      body,
	}

	// ── FIX: register the pending request BEFORE writing ──
	// Otherwise a fast tunnel response can arrive before we're listening,
	// and the response gets dropped — caller waits 10s and 504s.
	pending := &PendingRequest{
		ResponseChan: make(chan *models.Message, 1), // buffered: never blocks the reader
		Timeout:      time.Now().Add(15 * time.Second),
	}
	h.pendingMu.Lock()
	h.pendingRequests[requestID] = pending
	h.pendingMu.Unlock()

	defer func() {
		h.pendingMu.Lock()
		delete(h.pendingRequests, requestID)
		h.pendingMu.Unlock()
	}()

	// ── FIX: serialize all writes through safeWriteJSON ──
	if err := h.safeWriteJSON(connection.Conn, msg); err != nil {
		log.Printf("Failed to forward request: %v", err)
		c.JSON(http.StatusBadGateway, gin.H{"error": "Tunnel unavailable"})
		return
	}

	log.Printf("Forwarded %s %s to tunnel", c.Request.Method, requestPath)

	select {
	case response := <-pending.ResponseChan:
		if headers, ok := response.Headers.(map[string]interface{}); ok {
			for key, value := range headers {
				if hv, ok := value.(string); ok {
					// ── FIX: skip content-encoding and content-length ──
					// The tunnel client (Node.js) has already decompressed
					// the body before sending it back over the WebSocket.
					// Forwarding the compression header would make the browser
					// try to gunzip a plain-text body, producing garbled output.
					// content-length is also stale (compressed length ≠ body length).
					lk := strings.ToLower(key)
					if lk == "content-encoding" || lk == "content-length" {
						continue
					}
					c.Header(key, hv)
				}
			}
		}
		c.Status(response.StatusCode)
		if bodyStr, ok := response.Body.(string); ok {
			c.String(response.StatusCode, "%s", bodyStr)
		} else {
			c.JSON(response.StatusCode, response.Body)
		}
	case <-time.After(15 * time.Second):
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": "Tunnel timed out"})
	}
}

func (h *TunnelHandlers) handleResponse(msg *models.Message) {
	h.pendingMu.RLock()
	pending, exists := h.pendingRequests[msg.RequestID]
	h.pendingMu.RUnlock()
	if !exists {
		return
	}
	// Non-blocking send — the buffer holds one message, but if the caller
	// already timed out, drop on the floor rather than blocking the reader.
	select {
	case pending.ResponseChan <- msg:
	default:
		log.Printf("Dropped late response for %s", msg.RequestID)
	}
}

func getServerURL(r *http.Request) string {
	scheme := "https"
	if r.TLS == nil && r.Header.Get("X-Forwarded-Proto") == "" {
		scheme = "http"
	}
	if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
		scheme = p
	}
	return scheme + "://" + r.Host
}

func generateRandomID() string {
	b := make([]byte, 6)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func parsePort(portStr string) int {
	if port, err := strconv.Atoi(portStr); err == nil {
		return port
	}
	return 3000
}

// readBody reads up to max bytes from r, returning the raw slice.
func readBody(r interface{ Read([]byte) (int, error) }, max int) ([]byte, error) {
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	total := 0
	for {
		n, err := r.Read(tmp)
		if err != nil {
			if n > 0 {
				total += n
				if total > max {
					return nil, fmt.Errorf("body exceeds %d bytes", max)
				}
				buf = append(buf, tmp[:n]...)
			}
			if err.Error() == "EOF" {
				return buf, nil
			}
			return buf, err
		}
		if n > 0 {
			total += n
			if total > max {
				return nil, fmt.Errorf("body exceeds %d bytes", max)
			}
			buf = append(buf, tmp[:n]...)
		}
	}
}
