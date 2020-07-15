/*
Copyright 2019 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package events

import (
	"bytes"
	"time"
)

// GenerateSession generates session events starting with session start
// event, adds printEvents events and returns the result
func GenerateSession(printEvents int64) []AuditEvent {
	sessionStart := SessionStart{
		Metadata: Metadata{
			Index: 0,
			Type:  SessionStartEvent,
			ID:    "36cee9e9-9a80-4c32-9163-3d9241cdac7a",
			Code:  SessionStartCode,
			Time:  time.Date(2020, 03, 30, 15, 58, 54, 561*int(time.Millisecond), time.UTC),
		},
		ServerMetadata: ServerMetadata{
			ServerID: "a7c54b0c-469c-431e-af4d-418cd3ae9694",
			ServerLabels: map[string]string{
				"kernel": "5.3.0-42-generic",
				"date":   "Mon Mar 30 08:58:54 PDT 2020",
				"group":  "gravitational/devc",
			},
			ServerHostname:  "planet",
			ServerNamespace: "default",
		},
		SessionMetadata: SessionMetadata{
			SessionID: "5b3555dc-729f-11ea-b66a-507b9dd95841",
		},
		UserMetadata: UserMetadata{
			User:  "bob@example.com",
			Login: "bob",
		},
		ConnectionMetadata: ConnectionMetadata{
			LocalAddr:  "127.0.0.1:3022",
			RemoteAddr: "[::1]:37718",
		},
		TerminalSize: "80:25",
	}

	sessionEnd := SessionEnd{
		Metadata: Metadata{
			Index: 20,
			Type:  SessionEndEvent,
			ID:    "da455e0f-c27d-459f-a218-4e83b3db9426",
			Code:  SessionEndCode,
			Time:  time.Date(2020, 03, 30, 15, 58, 58, 999*int(time.Millisecond), time.UTC),
		},
		ServerMetadata: ServerMetadata{
			ServerID:        "a7c54b0c-469c-431e-af4d-418cd3ae9694",
			ServerNamespace: "default",
		},
		SessionMetadata: SessionMetadata{
			SessionID: "5b3555dc-729f-11ea-b66a-507b9dd95841",
		},
		UserMetadata: UserMetadata{
			User: "alice@example.com",
		},
		EnhancedRecording: true,
		Interactive:       true,
		Participants:      []string{"alice@example.com"},
		StartTime:         time.Date(2020, 03, 30, 15, 58, 54, 561*int(time.Millisecond), time.UTC),
		EndTime:           time.Date(2020, 03, 30, 15, 58, 58, 999*int(time.Millisecond), time.UTC),
	}

	events := []AuditEvent{&sessionStart}
	i := int64(0)
	for i = 0; i < printEvents; i++ {
		event := &SessionPrint{
			Metadata: Metadata{
				Index: int64(i) + 1,
				Type:  SessionPrintEvent,
				Time:  time.Date(2020, 03, 30, 15, 58, 56, 959*int(time.Millisecond), time.UTC),
			},
			ChunkIndex:        int64(i),
			DelayMilliseconds: int64(i),
			Offset:            int64(i),
			Data:              bytes.Repeat([]byte("hello"), int(i%177+1)),
		}
		event.Bytes = int64(len(event.Data))
		event.Time = event.Time.Add(time.Duration(i) * time.Millisecond)
		events = append(events, event)
	}
	i++
	sessionEnd.Metadata.Index = i
	events = append(events, &sessionEnd)
	return events
}
