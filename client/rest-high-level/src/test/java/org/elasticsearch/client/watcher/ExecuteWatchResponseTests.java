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

package org.elasticsearch.client.watcher;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.hamcrest.Matchers.is;

public class ExecuteWatchResponseTests extends ESTestCase {

    public static final String WATCH_ID_VALUE = "my_watch";
    public static final String NODE_VALUE = "my_node";
    public static final String TRIGGER_TYPE_VALUE = "manual";
    public static final String STATE_VALUE = "executed";
    public static final String STATE_KEY = "state";
    public static final String TRIGGER_EVENT_KEY = "trigger_event";
    public static final String TRIGGER_EVENT_TYPE_KEY = "type";
    public static final String MESSAGES_KEY = "messages";
    public static final String NODE_KEY = "node";
    public static final String WATCH_ID_KEY = "watch_id";

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            ExecuteWatchResponseTests::createTestInstance,
            this::toXContent,
            ExecuteWatchResponse::fromXContent)
            .supportsUnknownFields(true)
            .assertEqualsConsumer(this::assertEqualInstances)
            .assertToXContentEquivalence(false)
            .test();
    }

    private void assertEqualInstances(ExecuteWatchResponse expected, ExecuteWatchResponse actual) {
        assertThat(expected.getRecordId(), is(actual.getRecordId()));

        // This may have extra json, so lets just assume that if all of the original fields from the creation are there, then its equal
        // This is the same code that is in createTestInstance in this class.
        Map<String, Object> actualMap = actual.getRecordAsMap();
        assertThat(ObjectPath.eval(WATCH_ID_KEY, actualMap), is(WATCH_ID_VALUE));
        assertThat(ObjectPath.eval(NODE_KEY, actualMap), is(NODE_VALUE));
        List<Object> messages = ObjectPath.eval(MESSAGES_KEY, actualMap);
        assertThat(messages.size(), is(0));
        assertThat(ObjectPath.eval(TRIGGER_EVENT_KEY + "." + TRIGGER_EVENT_TYPE_KEY, actualMap), is(TRIGGER_TYPE_VALUE));
        assertThat(ObjectPath.eval(STATE_KEY, actualMap), is(STATE_VALUE));
    }

    private XContentBuilder toXContent(BytesReference bytes, XContentBuilder builder) throws IOException {
        // EMPTY is safe here because we never use namedObject
        try (InputStream stream = bytes.streamInput();
             XContentParser parser = createParser(JsonXContent.jsonXContent, stream)) {
            parser.nextToken();
            builder.generator().copyCurrentStructure(parser);
            return builder;
        }
    }

    private XContentBuilder toXContent(ExecuteWatchResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("_id", response.getRecordId());
        builder.field("watch_record");
        toXContent(response.getRecord(), builder);
        return builder.endObject();
    }

    private static ExecuteWatchResponse createTestInstance() {
        String id = "my_watch_0-2015-06-02T23:17:55.124Z";
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field(WATCH_ID_KEY, WATCH_ID_VALUE);
            builder.field(NODE_KEY, NODE_VALUE);
            builder.startArray(MESSAGES_KEY);
            builder.endArray();
            builder.startObject(TRIGGER_EVENT_KEY);
            builder.field(TRIGGER_EVENT_TYPE_KEY, TRIGGER_TYPE_VALUE);
            builder.endObject();
            builder.field(STATE_KEY, STATE_VALUE);
            builder.endObject();
            BytesReference bytes = BytesReference.bytes(builder);
            return new ExecuteWatchResponse(id, bytes);
        }
        catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
