/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.watcher.hlrc;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.client.AbstractHlrcXContentTestCase;
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
