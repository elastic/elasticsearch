/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.xcontent.XContentBuilder;

/**
 * A REST action listener that builds an {@link XContentBuilder} based response.
 */
public abstract class RestBuilderListener<Response> extends RestResponseListener<Response> {

    public RestBuilderListener(RestChannel channel) {
        super(channel);
    }

    @Override
    public final RestResponse buildResponse(Response response) throws Exception {
        try (XContentBuilder builder = channel.newBuilder()) {
            final RestResponse restResponse = buildResponse(response, builder);
            assert assertBuilderClosed(builder);
            assert restResponse.content() != null && restResponse.content().length() > 0 : "Use EmptyResponseListener for empty responses";
            return restResponse;
        }
    }

    /**
     * Builds a response to send back over the channel. Implementors should ensure that they close the provided {@link XContentBuilder}
     * using the {@link XContentBuilder#close()} method.
     */
    public abstract RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception;

    // pkg private method that we can override for testing
    boolean assertBuilderClosed(XContentBuilder xContentBuilder) {
        assert xContentBuilder.generator().isClosed() : "callers should ensure the XContentBuilder is closed themselves";
        return true;
    }
}
