/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.core;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public final class GetSourceResponseTests extends
    AbstractResponseTestCase<GetSourceResponseTests.SourceOnlyResponse, GetSourceResponse> {

    static class SourceOnlyResponse implements ToXContentObject {

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
        assertThat(clientInstance.getSource(), equalTo(Map.of("field", "value")));
    }
}
