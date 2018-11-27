/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.AbstractHlrcXContentTestCase;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;

import java.io.IOException;

public class ExecuteWatchResponseTests
    extends AbstractHlrcXContentTestCase<ExecuteWatchResponse, org.elasticsearch.client.watcher.ExecuteWatchResponse> {

    @Override
    public org.elasticsearch.client.watcher.ExecuteWatchResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.watcher.ExecuteWatchResponse.fromXContent(parser);
    }

    @Override
    public ExecuteWatchResponse convertHlrcToInternal(org.elasticsearch.client.watcher.ExecuteWatchResponse instance) {
        return new ExecuteWatchResponse(instance.getRecordId(), instance.getRecord(), XContentType.JSON);
    }

    @Override
    protected ExecuteWatchResponse createTestInstance() {
        String id = "my_watch_0-2015-06-02T23:17:55.124Z";
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("watch_id", "my_watch");
            builder.field("node", "my_node");
            builder.startArray("messages");
            builder.endArray();
            builder.startObject("trigger_event");
            builder.field("type", "manual");
            builder.endObject();
            builder.field("state", "executed");
            builder.endObject();
            BytesReference bytes = BytesReference.bytes(builder);
            return new ExecuteWatchResponse(id, bytes, XContentType.JSON);
        }
        catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    protected ExecuteWatchResponse doParseInstance(XContentParser parser) throws IOException {
        return ExecuteWatchResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
