/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ExecuteWatchResponseTests extends AbstractResponseTestCase<
    ExecuteWatchResponse, org.elasticsearch.client.watcher.ExecuteWatchResponse> {

    @Override
    protected ExecuteWatchResponse createServerTestInstance(XContentType xContentType) {
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
    protected org.elasticsearch.client.watcher.ExecuteWatchResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.watcher.ExecuteWatchResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(ExecuteWatchResponse serverTestInstance,
                                   org.elasticsearch.client.watcher.ExecuteWatchResponse clientInstance) {
        assertThat(clientInstance.getRecordId(), equalTo(serverTestInstance.getRecordId()));
        assertThat(clientInstance.getRecordAsMap(), equalTo(serverTestInstance.getRecordSource().getAsMap()));
    }
}
