/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.Predicate;

public class WatchStatusTests extends ESTestCase {

    public void testBasicParsing() throws IOException {
        int expectedVersion = randomIntBetween(0, 100);
        ExecutionState expectedExecutionState = randomFrom(ExecutionState.values());
        boolean expectedActive = randomBoolean();
        ActionStatus.AckStatus.State expectedAckState = randomFrom(ActionStatus.AckStatus.State.values());

        XContentBuilder builder = createTestXContent(expectedVersion, expectedExecutionState,
            expectedActive, expectedAckState);
        BytesReference bytes = BytesReference.bytes(builder);

        WatchStatus watchStatus = parse(builder.contentType(), bytes);

        assertEquals(expectedVersion, watchStatus.version());
        assertEquals(expectedExecutionState, watchStatus.getExecutionState());

        assertEquals(Instant.ofEpochMilli(1432663467763L).atZone(ZoneOffset.UTC), watchStatus.lastChecked());
        assertEquals(ZonedDateTime.parse("2015-05-26T18:04:27.763Z"), watchStatus.lastMetCondition());

        WatchStatus.State watchState = watchStatus.state();
        assertEquals(expectedActive, watchState.isActive());
        assertEquals(ZonedDateTime.parse("2015-05-26T18:04:27.723Z"), watchState.getTimestamp());

        ActionStatus actionStatus = watchStatus.actionStatus("test_index");
        assertNotNull(actionStatus);

        ActionStatus.AckStatus ackStatus = actionStatus.ackStatus();
        assertEquals(ZonedDateTime.parse("2015-05-26T18:04:27.763Z"), ackStatus.timestamp());
        assertEquals(expectedAckState, ackStatus.state());

        ActionStatus.Execution lastExecution = actionStatus.lastExecution();
        assertEquals(ZonedDateTime.parse("2015-05-25T18:04:27.733Z"), lastExecution.timestamp());
        assertFalse(lastExecution.successful());
        assertEquals("failed to send email", lastExecution.reason());

        ActionStatus.Execution lastSuccessfulExecution = actionStatus.lastSuccessfulExecution();
        assertEquals(ZonedDateTime.parse("2015-05-25T18:04:27.773Z"), lastSuccessfulExecution.timestamp());
        assertTrue(lastSuccessfulExecution.successful());
        assertNull(lastSuccessfulExecution.reason());

        ActionStatus.Throttle lastThrottle = actionStatus.lastThrottle();
        assertEquals(ZonedDateTime.parse("2015-04-25T18:05:23.445Z"), lastThrottle.timestamp());
        assertEquals("throttling interval is set to [5 seconds] ...", lastThrottle.reason());
    }

    public void testParsingWithUnknownKeys() throws IOException {
        int expectedVersion = randomIntBetween(0, 100);
        ExecutionState expectedExecutionState = randomFrom(ExecutionState.values());
        boolean expectedActive = randomBoolean();
        ActionStatus.AckStatus.State expectedAckState = randomFrom(ActionStatus.AckStatus.State.values());

        XContentBuilder builder = createTestXContent(expectedVersion, expectedExecutionState,
            expectedActive, expectedAckState);
        BytesReference bytes = BytesReference.bytes(builder);

        Predicate<String> excludeFilter = field -> field.equals("actions");
        BytesReference bytesWithRandomFields = XContentTestUtils.insertRandomFields(
            builder.contentType(), bytes, excludeFilter, random());

        WatchStatus watchStatus = parse(builder.contentType(), bytesWithRandomFields);

        assertEquals(expectedVersion, watchStatus.version());
        assertEquals(expectedExecutionState, watchStatus.getExecutionState());
    }

    public void testOptionalFieldsParsing() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(contentType).startObject()
            .field("version", 42)
            .startObject("actions")
                .startObject("test_index")
                    .startObject("ack")
                        .field("timestamp", "2015-05-26T18:04:27.763Z")
                        .field("state", "ackable")
                    .endObject()
                    .startObject("last_execution")
                        .field("timestamp", "2015-05-25T18:04:27.733Z")
                        .field("successful", false)
                        .field("reason", "failed to send email")
                    .endObject()
                .endObject()
            .endObject()
        .endObject();
        BytesReference bytes = BytesReference.bytes(builder);

        WatchStatus watchStatus = parse(builder.contentType(), bytes);

        assertEquals(42, watchStatus.version());
        assertNull(watchStatus.getExecutionState());
        assertFalse(watchStatus.checked());
    }

    private XContentBuilder createTestXContent(int version,
                                               ExecutionState executionState,
                                               boolean active,
                                               ActionStatus.AckStatus.State ackState) throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        return XContentFactory.contentBuilder(contentType).startObject()
            .field("version", version)
            .field("execution_state", executionState)
            .field("last_checked", 1432663467763L)
            .field("last_met_condition", "2015-05-26T18:04:27.763Z")
            .startObject("state")
                .field("active", active)
                .field("timestamp", "2015-05-26T18:04:27.723Z")
            .endObject()
            .startObject("actions")
                .startObject("test_index")
                    .startObject("ack")
                        .field("timestamp", "2015-05-26T18:04:27.763Z")
                        .field("state", ackState)
                    .endObject()
                    .startObject("last_execution")
                        .field("timestamp", "2015-05-25T18:04:27.733Z")
                        .field("successful", false)
                        .field("reason", "failed to send email")
                    .endObject()
                    .startObject("last_successful_execution")
                        .field("timestamp", "2015-05-25T18:04:27.773Z")
                        .field("successful", true)
                    .endObject()
                    .startObject("last_throttle")
                        .field("timestamp", "2015-04-25T18:05:23.445Z")
                        .field("reason", "throttling interval is set to [5 seconds] ...")
                    .endObject()
                .endObject()
            .endObject()
        .endObject();
    }

    private WatchStatus parse(XContentType contentType, BytesReference bytes) throws IOException {
        XContentParser parser = XContentFactory.xContent(contentType)
            .createParser(NamedXContentRegistry.EMPTY, null, bytes.streamInput());
        parser.nextToken();

        return WatchStatus.parse(parser);
    }
}
