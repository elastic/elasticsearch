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

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.nullValue;


public class GrokProcessorGetActionTests extends ESTestCase {
    private static final Map<String, String> TEST_PATTERNS = Collections.singletonMap("PATTERN", "foo");

    public void testRequest() throws Exception {
        GrokProcessorGetAction.Request request = new GrokProcessorGetAction.Request();
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GrokProcessorGetAction.Request otherRequest = new GrokProcessorGetAction.Request();
        otherRequest.readFrom(streamInput);
        assertThat(otherRequest.validate(), nullValue());
    }

    public void testResponseSerialization() throws Exception {
        GrokProcessorGetAction.Response response = new GrokProcessorGetAction.Response(TEST_PATTERNS);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GrokProcessorGetAction.Response otherResponse = new GrokProcessorGetAction.Response(null);
        otherResponse.readFrom(streamInput);
        assertThat(response.getGrokPatterns(), equalTo(TEST_PATTERNS));
        assertThat(response.getGrokPatterns(), equalTo(otherResponse.getGrokPatterns()));
    }

    @SuppressWarnings("unchecked")
    public void testResponseToXContent() throws Exception {
        GrokProcessorGetAction.Response response = new GrokProcessorGetAction.Response(TEST_PATTERNS);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            Map<String, Object> converted = XContentHelper.convertToMap(builder.bytes(), false, builder.contentType()).v2();
            Map<String, String> patterns = (Map<String, String>) converted.get("patterns");
            assertThat(patterns.size(), equalTo(1));
            assertThat(patterns.get("PATTERN"), equalTo("foo"));
        }
    }
}
