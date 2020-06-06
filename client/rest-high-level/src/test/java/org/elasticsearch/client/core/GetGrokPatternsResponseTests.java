/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.core;

import org.apache.http.HttpHost;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static org.hamcrest.CoreMatchers.equalTo;

public final class GetGrokPatternsResponseTests extends AbstractResponseTestCase<GetGrokPatternsResponseTests.TestGrokPatternResponse, GetGrokPatternsResponse> {

    @Override
    protected TestGrokPatternResponse createServerTestInstance(XContentType xContentType) {
        Map<String, String> grokPatterns = new HashMap<>();
        grokPatterns.put("key", "value");
        return new TestGrokPatternResponse(grokPatterns);
    }

    @Override
    protected GetGrokPatternsResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetGrokPatternsResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(TestGrokPatternResponse serverTestInstance, GetGrokPatternsResponse clientInstance) {
        assertThat(clientInstance.getGrokPatterns(), equalTo(Map.of("key", "value")));
    }

    class TestGrokPatternResponse implements ToXContent {
        private final Map<String, String> grokPatterns;

        TestGrokPatternResponse(Map<String, String> grokPatterns){
            this.grokPatterns = grokPatterns;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("patterns");
            builder.map(grokPatterns);
            builder.endObject();
            return builder;
        }
    }
}



