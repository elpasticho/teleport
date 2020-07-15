package test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gravitational/teleport/lib/events"
	"github.com/gravitational/teleport/lib/fixtures"
	"github.com/gravitational/teleport/lib/session"

	"gopkg.in/check.v1"
)

// Stream tests stream upload and subsequent download and reads the results
func (s *HandlerSuite) Stream(c *check.C) {
	ctx := context.TODO()
	id := session.NewID()

	streamer, err := events.NewProtoStreamer(events.ProtoStreamerConfig{
		Uploader: s.Handler,
	})
	c.Assert(err, check.IsNil)

	stream, err := streamer.CreateAuditStream(ctx, id)
	c.Assert(err, check.IsNil)

	inEvents := []events.AuditEvent{&SessionStart, &SessionPrint, &SessionEnd}
	for _, event := range inEvents {
		err := stream.EmitAuditEvent(ctx, event)
		c.Assert(err, check.IsNil)
	}

	err = stream.Complete(ctx)
	c.Assert(err, check.IsNil)

	dir := c.MkDir()
	f, err := os.Create(filepath.Join(dir, string(id)))
	c.Assert(err, check.IsNil)
	defer f.Close()

	err = s.Handler.Download(context.TODO(), id, f)
	c.Assert(err, check.IsNil)

	_, err = f.Seek(0, 0)
	c.Assert(err, check.IsNil)

	reader := events.NewProtoReader(f)
	out, err := reader.ReadAll(ctx)
	c.Assert(err, check.IsNil)

	fixtures.DeepCompareSlices(c, inEvents, out)
}

func max(vars ...int) int {
	m := vars[0]
	for _, zz := range vars {
		if zz > m {
			m = zz
		}
	}
	return m
}

var (
	// SessionStart is a session start event
	SessionStart = events.SessionStart{
		Metadata: events.Metadata{
			Index: 0,
			Type:  events.SessionStartEvent,
			ID:    "36cee9e9-9a80-4c32-9163-3d9241cdac7a",
			Code:  events.SessionStartCode,
			Time:  time.Date(2020, 03, 30, 15, 58, 54, 561*int(time.Millisecond), time.UTC),
		},
		ServerMetadata: events.ServerMetadata{
			ServerID: "a7c54b0c-469c-431e-af4d-418cd3ae9694",
			ServerLabels: map[string]string{
				"kernel": "5.3.0-42-generic",
				"date":   "Mon Mar 30 08:58:54 PDT 2020",
				"group":  "gravitational/devc",
			},
			ServerHostname:  "planet",
			ServerNamespace: "default",
		},
		SessionMetadata: events.SessionMetadata{
			SessionID: "5b3555dc-729f-11ea-b66a-507b9dd95841",
		},
		UserMetadata: events.UserMetadata{
			User:  "bob@example.com",
			Login: "bob",
		},
		ConnectionMetadata: events.ConnectionMetadata{
			LocalAddr:  "127.0.0.1:3022",
			RemoteAddr: "[::1]:37718",
		},
		TerminalSize: "80:25",
	}
	// SessionPrint is a session print event
	SessionPrint = events.SessionPrint{
		Metadata: events.Metadata{
			Index: 11,
			Type:  events.SessionPrintEvent,
			Time:  time.Date(2020, 03, 30, 15, 58, 56, 959*int(time.Millisecond), time.UTC),
		},
		ChunkIndex:        9,
		Bytes:             1551,
		Data:              []byte(strings.Repeat("c", 1551)),
		DelayMilliseconds: 2284,
		Offset:            1957,
	}
	// SessionEnd is a session end event
	SessionEnd = events.SessionEnd{
		Metadata: events.Metadata{
			Index: 20,
			Type:  events.SessionEndEvent,
			ID:    "da455e0f-c27d-459f-a218-4e83b3db9426",
			Code:  events.SessionEndCode,
			Time:  time.Date(2020, 03, 30, 15, 58, 58, 999*int(time.Millisecond), time.UTC),
		},
		ServerMetadata: events.ServerMetadata{
			ServerID:        "a7c54b0c-469c-431e-af4d-418cd3ae9694",
			ServerNamespace: "default",
		},
		SessionMetadata: events.SessionMetadata{
			SessionID: "5b3555dc-729f-11ea-b66a-507b9dd95841",
		},
		UserMetadata: events.UserMetadata{
			User: "alice@example.com",
		},
		EnhancedRecording: true,
		Interactive:       true,
		Participants:      []string{"alice@example.com"},
	}
)
