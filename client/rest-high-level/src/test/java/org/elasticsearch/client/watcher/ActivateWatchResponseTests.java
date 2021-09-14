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
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * Basic unit tests for {@link ActivateWatchResponse}.
 *
 * Note that we only sanity check watch status parsing here, as there
 * are dedicated tests for it in {@link WatchStatusTests}.
 */
public class ActivateWatchResponseTests extends ESTestCase {

    public void testBasicParsing() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(contentType).startObject()
            .startObject("status")
                .field("version", 42)
                .field("execution_state", ExecutionState.ACKNOWLEDGED)
                .startObject("state")
                   .field("active", false)
                .endObject()
            .endObject()
        .endObject();
        BytesReference bytes = BytesReference.bytes(builder);

        ActivateWatchResponse response = parse(builder.contentType(), bytes);
        WatchStatus status = response.getStatus();
        assertNotNull(status);
        assertEquals(42, status.version());
        assertEquals(ExecutionState.ACKNOWLEDGED, status.getExecutionState());
        assertFalse(status.state().isActive());
    }

    public void testParsingWithMissingStatus() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(contentType).startObject().endObject();
        BytesReference bytes = BytesReference.bytes(builder);

        expectThrows(IllegalArgumentException.class, () -> parse(builder.contentType(), bytes));
    }

    public void testParsingWithNullStatus() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(contentType).startObject()
            .nullField("status")
        .endObject();
        BytesReference bytes = BytesReference.bytes(builder);

        expectThrows(XContentParseException.class, () -> parse(builder.contentType(), bytes));
    }

    public void testParsingWithUnknownKeys() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(contentType).startObject()
            .startObject("status")
                .field("version", 42)
                .field("execution_state", ExecutionState.ACKNOWLEDGED)
                .startObject("state")
                  .field("active", true)
                .endObject()
            .endObject()
        .endObject();
        BytesReference bytes = BytesReference.bytes(builder);

        Predicate<String> excludeFilter = field -> field.equals("status.actions");
        BytesReference bytesWithRandomFields = XContentTestUtils.insertRandomFields(
            builder.contentType(), bytes, excludeFilter, random());

        ActivateWatchResponse response = parse(builder.contentType(), bytesWithRandomFields);
        WatchStatus status = response.getStatus();
        assertNotNull(status);
        assertEquals(42, status.version());
        assertEquals(ExecutionState.ACKNOWLEDGED, status.getExecutionState());
        assertTrue(status.state().isActive());
    }

    private ActivateWatchResponse parse(XContentType contentType, BytesReference bytes) throws IOException {
        XContentParser parser = XContentFactory.xContent(contentType)
            .createParser(NamedXContentRegistry.EMPTY, null, bytes.streamInput());
        parser.nextToken();
        return ActivateWatchResponse.fromXContent(parser);
    }
}
