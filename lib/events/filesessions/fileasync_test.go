/*
Copyright 2020 Gravitational, Inc.

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

package filesessions

import (
	"bytes"
	"context"
	"go.uber.org/atomic"
	"testing"
	"time"

	"github.com/gravitational/teleport/lib/events"
	"github.com/gravitational/teleport/lib/fixtures"
	"github.com/gravitational/teleport/lib/session"
	"github.com/gravitational/teleport/lib/utils"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	log "github.com/sirupsen/logrus"

	"gopkg.in/check.v1"
)

type UploaderSuite struct {
}

var _ = check.Suite(&UploaderSuite{})

func (s *UploaderSuite) SetUpSuite(c *check.C) {
	utils.InitLoggerForTests(testing.Verbose())
}

// TestUploadOK verifies successfull upload run
func (s *UploaderSuite) TestUploadOK(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clock := clockwork.NewFakeClock()

	eventsC := make(chan events.UploadEvent, 100)
	memUploader := events.NewMemoryUploader(eventsC)
	streamer, err := events.NewProtoStreamer(events.ProtoStreamerConfig{
		Uploader: memUploader,
	})
	c.Assert(err, check.IsNil)

	scanDir := c.MkDir()
	scanPeriod := 10 * time.Second
	uploader, err := NewUploader(UploaderConfig{
		Context:    ctx,
		ScanDir:    scanDir,
		ScanPeriod: scanPeriod,
		Streamer:   streamer,
		Clock:      clock,
	})
	c.Assert(err, check.IsNil)
	go uploader.Serve()
	// wait until uploader blocks on the clock
	clock.BlockUntil(1)

	defer uploader.Close()

	fileStreamer, err := NewStreamer(scanDir)
	c.Assert(err, check.IsNil)

	inEvents := events.GenerateSession(1024)
	sid := inEvents[0].(events.SessionMetadataGetter).GetSessionID()

	emitStream(ctx, c, fileStreamer, inEvents)

	// initiate the scan by advancing clock past
	// block period
	clock.Advance(scanPeriod + time.Second)

	var event events.UploadEvent
	select {
	case event = <-eventsC:
		c.Assert(event.SessionID, check.Equals, sid)
		c.Assert(event.Error, check.IsNil)
	case <-ctx.Done():
		c.Fatalf("Timeout waiting for async upload, try `go test -v` to get more logs for details")
	}

	// read the upload and make sure the data is equal
	outEvents := readStream(ctx, c, event.UploadID, memUploader)

	fixtures.DeepCompareSlices(c, inEvents, outEvents)
}

// TestUploadResume verifies successfull upload run after the stream has been interrupted
func (s *UploaderSuite) TestUploadResume(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clock := clockwork.NewFakeClock()
	eventsC := make(chan events.UploadEvent, 100)
	memUploader := events.NewMemoryUploader(eventsC)
	streamer, err := events.NewProtoStreamer(events.ProtoStreamerConfig{
		Uploader: memUploader,
	})
	c.Assert(err, check.IsNil)

	streamResumed := atomic.NewUint64(0)
	terminateConnection := atomic.NewUint64(1)

	callbackStreamer, err := events.NewCallbackStreamer(events.CallbackStreamerConfig{
		Inner: streamer,
		OnEmitAuditEvent: func(ctx context.Context, sid session.ID, event events.AuditEvent) error {
			if event.GetIndex() > 500 && terminateConnection.CAS(1, 0) == true {
				log.Debugf("Terminating connection at event %v", event.GetIndex())
				return trace.ConnectionProblem(nil, "connection terminated")
			}
			return nil
		},
		OnResumeAuditStream: func(ctx context.Context, sid session.ID, uploadID string) error {
			streamResumed.Store(1)
			return nil
		},
	})

	scanDir := c.MkDir()
	scanPeriod := 10 * time.Second
	uploader, err := NewUploader(UploaderConfig{
		EventsC:    eventsC,
		Context:    ctx,
		ScanDir:    scanDir,
		ScanPeriod: scanPeriod,
		Streamer:   callbackStreamer,
		Clock:      clock,
	})
	c.Assert(err, check.IsNil)
	go uploader.Serve()
	// wait until uploader blocks on the clock
	clock.BlockUntil(1)

	defer uploader.Close()

	fileStreamer, err := NewStreamer(scanDir)
	c.Assert(err, check.IsNil)

	inEvents := events.GenerateSession(1024)
	sid := inEvents[0].(events.SessionMetadataGetter).GetSessionID()

	emitStream(ctx, c, fileStreamer, inEvents)

	// initiate the scan by advancing clock past
	// block period
	clock.Advance(scanPeriod + time.Second)

	// wait for upload failure
	var event events.UploadEvent
	select {
	case event = <-eventsC:
		c.Assert(event.SessionID, check.Equals, sid)
		c.Assert(event.Error, check.FitsTypeOf, trace.ConnectionProblem(nil, "connection problem"))
	case <-ctx.Done():
		c.Fatalf("Timeout waiting for async upload, try `go test -v` to get more logs for details")
	}

	// initiate the second attempt
	clock.BlockUntil(1)
	clock.Advance(scanPeriod + time.Second)

	// wait for upload success
	select {
	case event = <-eventsC:
		c.Assert(event.SessionID, check.Equals, sid)
		c.Assert(event.Error, check.IsNil)
	case <-ctx.Done():
		c.Fatalf("Timeout waiting for async upload, try `go test -v` to get more logs for details")
	}

	// read the upload and make sure the data is equal
	outEvents := readStream(ctx, c, event.UploadID, memUploader)

	fixtures.DeepCompareSlices(c, inEvents, outEvents)
}

// emitStream creates and sends the session stream
func emitStream(ctx context.Context, c *check.C, streamer events.Streamer, inEvents []events.AuditEvent) {
	sid := inEvents[0].(events.SessionMetadataGetter).GetSessionID()

	stream, err := streamer.CreateAuditStream(ctx, session.ID(sid))
	c.Assert(err, check.IsNil)
	for _, event := range inEvents {
		err := stream.EmitAuditEvent(ctx, event)
		c.Assert(err, check.IsNil)
	}
	err = stream.Complete(ctx)
	c.Assert(err, check.IsNil)
}

// readStream reads and decodes the audit stream from uploadID
func readStream(ctx context.Context, c *check.C, uploadID string, uploader *events.MemoryUploader) []events.AuditEvent {
	parts, err := uploader.GetParts(uploadID)
	c.Assert(err, check.IsNil)

	var outEvents []events.AuditEvent
	for _, part := range parts {
		reader := events.NewProtoReader(bytes.NewReader(part))
		out, err := reader.ReadAll(ctx)
		c.Assert(err, check.IsNil, check.Commentf("part crash %#v", part))
		outEvents = append(outEvents, out...)
	}
	return outEvents
}
