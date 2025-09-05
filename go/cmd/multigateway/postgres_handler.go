// Copyright 2025 The Multigres Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/multigres/multigres/go/postgres"
)

// PostgresHandler implements the postgres.Handler interface for multigateway
type PostgresHandler struct {
	logger          *slog.Logger
	poolerDiscovery *PoolerDiscovery
}

// NewPostgresHandler creates a new PostgreSQL protocol handler
func NewPostgresHandler(logger *slog.Logger, poolerDiscovery *PoolerDiscovery) *PostgresHandler {
	return &PostgresHandler{
		logger:          logger,
		poolerDiscovery: poolerDiscovery,
	}
}

// HandleStartup is called when a client connects
func (h *PostgresHandler) HandleStartup(ctx context.Context, conn *postgres.Connection, msg *postgres.StartupMessage) error {
	h.logger.Info("Client startup",
		"conn_id", conn.ID,
		"user", conn.User,
		"database", conn.Database,
		"application_name", msg.Parameters["application_name"],
	)
	
	// TODO: Validate user and database
	// TODO: Select appropriate multipooler based on database
	
	return nil
}

// HandleQuery processes a simple query
func (h *PostgresHandler) HandleQuery(ctx context.Context, conn *postgres.Connection, query string) error {
	h.logger.Debug("Handling query",
		"conn_id", conn.ID,
		"query", query,
	)
	
	// Normalize query for checking
	normalizedQuery := strings.TrimSpace(strings.ToUpper(query))
	
	// Handle special queries locally
	if normalizedQuery == "SELECT 1" || normalizedQuery == "SELECT 1;" {
		// Simple health check query
		return h.handleSelectOne(conn)
	}
	
	if strings.HasPrefix(normalizedQuery, "SELECT VERSION()") {
		// Version query
		return h.handleSelectVersion(conn)
	}
	
	if strings.HasPrefix(normalizedQuery, "SHOW") {
		// SHOW commands
		return h.handleShowCommand(conn, query)
	}
	
	if normalizedQuery == "" {
		// Empty query
		return conn.Writer.WriteEmptyQueryResponse()
	}
	
	// TODO: Forward query to appropriate multipooler
	// For now, return an error
	return h.sendError(conn, fmt.Errorf("query forwarding not yet implemented"))
}

// handleSelectOne handles the "SELECT 1" query
func (h *PostgresHandler) handleSelectOne(conn *postgres.Connection) error {
	// Send row description
	rowDesc := postgres.CreateSimpleRowDescription("?column?", postgres.TypeOIDInt4)
	if err := conn.Writer.WriteMessage(postgres.MsgTypeRowDescription, rowDesc); err != nil {
		return err
	}
	
	// Send data row
	dataRow := postgres.CreateSimpleDataRow("1")
	if err := conn.Writer.WriteMessage(postgres.MsgTypeDataRow, dataRow); err != nil {
		return err
	}
	
	// Send command complete
	if err := conn.Writer.WriteCommandComplete("SELECT 1"); err != nil {
		return err
	}
	
	// Send ready for query
	return conn.Writer.WriteReadyForQuery(conn.TxStatus)
}

// handleSelectVersion handles the "SELECT version()" query
func (h *PostgresHandler) handleSelectVersion(conn *postgres.Connection) error {
	// Send row description
	rowDesc := postgres.CreateSimpleRowDescription("version", postgres.TypeOIDText)
	if err := conn.Writer.WriteMessage(postgres.MsgTypeRowDescription, rowDesc); err != nil {
		return err
	}
	
	// Send data row with version string
	version := "PostgreSQL 15.0 (Multigres multigateway)"
	dataRow := postgres.CreateSimpleDataRow(version)
	if err := conn.Writer.WriteMessage(postgres.MsgTypeDataRow, dataRow); err != nil {
		return err
	}
	
	// Send command complete
	if err := conn.Writer.WriteCommandComplete("SELECT 1"); err != nil {
		return err
	}
	
	// Send ready for query
	return conn.Writer.WriteReadyForQuery(conn.TxStatus)
}

// handleShowCommand handles SHOW commands
func (h *PostgresHandler) handleShowCommand(conn *postgres.Connection, query string) error {
	// Parse the SHOW command
	parts := strings.Fields(strings.ToUpper(query))
	if len(parts) < 2 {
		return h.sendError(conn, fmt.Errorf("invalid SHOW command"))
	}
	
	var paramName, paramValue string
	
	switch parts[1] {
	case "SERVER_VERSION":
		paramName = "server_version"
		paramValue = "15.0 (Multigres)"
	case "SERVER_ENCODING":
		paramName = "server_encoding"
		paramValue = "UTF8"
	case "CLIENT_ENCODING":
		paramName = "client_encoding"
		paramValue = "UTF8"
	case "TIMEZONE", "TIME":
		if len(parts) > 2 && parts[2] == "ZONE" {
			paramName = "TimeZone"
			paramValue = "UTC"
		} else {
			return h.sendError(conn, fmt.Errorf("unknown SHOW parameter: %s", parts[1]))
		}
	default:
		// Try to return a generic error for unknown parameters
		return h.sendError(conn, fmt.Errorf("unknown SHOW parameter: %s", parts[1]))
	}
	
	// Send row description
	rowDesc := postgres.CreateSimpleRowDescription(paramName, postgres.TypeOIDText)
	if err := conn.Writer.WriteMessage(postgres.MsgTypeRowDescription, rowDesc); err != nil {
		return err
	}
	
	// Send data row
	dataRow := postgres.CreateSimpleDataRow(paramValue)
	if err := conn.Writer.WriteMessage(postgres.MsgTypeDataRow, dataRow); err != nil {
		return err
	}
	
	// Send command complete
	if err := conn.Writer.WriteCommandComplete("SHOW"); err != nil {
		return err
	}
	
	// Send ready for query
	return conn.Writer.WriteReadyForQuery(conn.TxStatus)
}


// HandleParse processes a Parse message (extended query protocol)
func (h *PostgresHandler) HandleParse(ctx context.Context, conn *postgres.Connection, msg *postgres.Message) error {
	h.logger.Debug("Parse message received", "conn_id", conn.ID)
	
	// For now, just send ParseComplete
	// TODO: Actually parse and store the prepared statement
	return conn.Writer.WriteMessage(postgres.MsgTypeParseComplete, nil)
}

// HandleBind processes a Bind message (extended query protocol)
func (h *PostgresHandler) HandleBind(ctx context.Context, conn *postgres.Connection, msg *postgres.Message) error {
	h.logger.Debug("Bind message received", "conn_id", conn.ID)
	
	// For now, just send BindComplete
	// TODO: Actually bind parameters to the prepared statement
	return conn.Writer.WriteMessage(postgres.MsgTypeBindComplete, nil)
}

// HandleExecute processes an Execute message (extended query protocol)
func (h *PostgresHandler) HandleExecute(ctx context.Context, conn *postgres.Connection, msg *postgres.Message) error {
	h.logger.Debug("Execute message received", "conn_id", conn.ID)
	
	// TODO: Execute the bound statement
	// For now, return an error
	return h.sendError(conn, fmt.Errorf("extended query protocol not yet implemented"))
}

// HandleDescribe processes a Describe message
func (h *PostgresHandler) HandleDescribe(ctx context.Context, conn *postgres.Connection, msg *postgres.Message) error {
	h.logger.Debug("Describe message received", "conn_id", conn.ID)
	
	// TODO: Describe the statement or portal
	// For now, send NoData
	return conn.Writer.WriteMessage(postgres.MsgTypeNoData, nil)
}

// HandleClose processes a Close message
func (h *PostgresHandler) HandleClose(ctx context.Context, conn *postgres.Connection, msg *postgres.Message) error {
	h.logger.Debug("Close message received", "conn_id", conn.ID)
	
	// TODO: Close the statement or portal
	// For now, just send CloseComplete
	return conn.Writer.WriteMessage(postgres.MsgTypeCloseComplete, nil)
}

// HandleTerminate is called when the client disconnects
func (h *PostgresHandler) HandleTerminate(ctx context.Context, conn *postgres.Connection) {
	h.logger.Debug("Client terminating", "conn_id", conn.ID)
	
	// TODO: Clean up any resources associated with this connection
}

// sendError sends an error response to the client
func (h *PostgresHandler) sendError(conn *postgres.Connection, err error) error {
	fields := make(map[byte]string)
	fields[postgres.ErrorFieldSeverity] = "ERROR"
	fields[postgres.ErrorFieldMessage] = err.Error()
	fields[postgres.ErrorFieldCode] = "58000" // system_error
	
	if err := conn.Writer.WriteErrorResponse(fields); err != nil {
		return err
	}
	
	// Send ready for query
	return conn.Writer.WriteReadyForQuery(conn.TxStatus)
}

