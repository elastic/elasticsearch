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

package org.elasticsearch.client.core;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public final class GetSourceResponseTests extends
    AbstractResponseTestCase<GetSourceResponseTests.SourceOnlyResponse, GetSourceResponse> {

    static class SourceOnlyResponse implements ToXContent {

        private final BytesReference source;

        SourceOnlyResponse(BytesReference source) {
            this.source = source;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // this implementation copied from RestGetSourceAction.RestGetSourceResponseListener::buildResponse
            try (InputStream stream = source.streamInput()) {
                builder.rawValue(stream, XContentHelper.xContentType(source));
            }
            return builder;
        }
    }

    @Override
    protected SourceOnlyResponse createServerTestInstance(XContentType xContentType) {
        BytesReference source = new BytesArray("{\"field\":\"value\"}");
        return new SourceOnlyResponse(source);
    }

    @Override
    protected GetSourceResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetSourceResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(SourceOnlyResponse serverTestInstance, GetSourceResponse clientInstance) {
        Map<String, Object> expected = new HashMap<>();
        expected.put("field", "value");

        assertThat(clientInstance.getSource(), equalTo(serverTestInstance.source));
        assertThat(clientInstance.getSource(), equalTo(expected));
    }
}
