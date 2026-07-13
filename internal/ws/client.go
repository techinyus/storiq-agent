package ws

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pingInterval   = 30 * time.Second
	reconnectDelay = 5 * time.Second
)

// QueryExecutor runs a SQL query against the client's local database.
type QueryExecutor interface {
	ExecuteQuery(query string, params []interface{}) ([]map[string]interface{}, error)
}

type Client struct {
	url          string
	secret       string
	agentVersion string
	executor     QueryExecutor

	writeMu sync.Mutex
}

func NewClient(url, secret, agentVersion string, executor QueryExecutor) *Client {
	return &Client{
		url:          url,
		secret:       secret,
		agentVersion: agentVersion,
		executor:     executor,
	}
}

type authMessage struct {
	Type         string `json:"type"`
	Secret       string `json:"secret"`
	AgentVersion string `json:"agent_version"`
}

type authResponse struct {
	Type string `json:"type"`
}

type queryMessage struct {
	Type    string        `json:"type"`
	QueryID string        `json:"query_id"`
	SQL     string        `json:"sql"`
	Params  []interface{} `json:"params"`
}

type resultMessage struct {
	Type    string                   `json:"type"`
	QueryID string                   `json:"query_id"`
	Rows    []map[string]interface{} `json:"rows"`
	Error   *string                  `json:"error"`
}

type pingMessage struct {
	Type string `json:"type"`
}

// Run connects to the server and keeps reconnecting on failure until the process exits.
func (c *Client) Run() {
	for {
		if err := c.connectAndServe(); err != nil {
			log.Printf("[ws] connection error: %v", err)
		}
		log.Printf("[ws] reconnecting in %s...", reconnectDelay)
		time.Sleep(reconnectDelay)
	}
}

func (c *Client) connectAndServe() error {
	log.Printf("[ws] connecting to %s", c.url)
	conn, _, err := websocket.DefaultDialer.Dial(c.url, nil)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	defer conn.Close()

	log.Println("[ws] connected")

	if err := c.writeJSON(conn, authMessage{
		Type:         "auth",
		Secret:       c.secret,
		AgentVersion: c.agentVersion,
	}); err != nil {
		return fmt.Errorf("failed to send auth message: %w", err)
	}

	_, msg, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read auth response: %w", err)
	}

	var resp authResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return fmt.Errorf("failed to parse auth response: %w", err)
	}
	if resp.Type != "auth_ok" {
		return fmt.Errorf("authentication rejected by server (type=%q)", resp.Type)
	}
	log.Println("[ws] authenticated")

	done := make(chan struct{})
	var pingWg sync.WaitGroup
	pingWg.Add(1)
	go func() {
		defer pingWg.Done()
		c.pingLoop(conn, done)
	}()
	defer func() {
		close(done)
		pingWg.Wait()
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}

		var base struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(message, &base); err != nil {
			log.Printf("[ws] failed to parse incoming message: %v", err)
			continue
		}

		switch base.Type {
		case "query":
			var q queryMessage
			if err := json.Unmarshal(message, &q); err != nil {
				log.Printf("[ws] failed to parse query message: %v", err)
				continue
			}
			go c.handleQuery(conn, q)
		default:
			log.Printf("[ws] ignoring unknown message type: %q", base.Type)
		}
	}
}

func (c *Client) pingLoop(conn *websocket.Conn, done chan struct{}) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.writeJSON(conn, pingMessage{Type: "ping"}); err != nil {
				log.Printf("[ws] failed to send ping: %v", err)
				return
			}
		case <-done:
			return
		}
	}
}

func (c *Client) handleQuery(conn *websocket.Conn, q queryMessage) {
	result := resultMessage{
		Type:    "result",
		QueryID: q.QueryID,
		Rows:    []map[string]interface{}{},
	}

	rows, err := c.executor.ExecuteQuery(q.SQL, q.Params)
	if err != nil {
		log.Printf("[ws] query %s failed: %v", q.QueryID, err)
		errStr := err.Error()
		result.Error = &errStr
	} else {
		result.Rows = rows
	}

	if err := c.writeJSON(conn, result); err != nil {
		log.Printf("[ws] failed to send result for query %s: %v", q.QueryID, err)
	}
}

func (c *Client) writeJSON(conn *websocket.Conn, v interface{}) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return conn.WriteJSON(v)
}
