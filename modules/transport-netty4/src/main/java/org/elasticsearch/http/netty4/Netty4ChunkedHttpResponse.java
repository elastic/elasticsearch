/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestStatus;

/**
 * A http response that will be transferred via chunked encoding when handled by {@link Netty4HttpPipeliningHandler}.
 */
public final class Netty4ChunkedHttpResponse extends DefaultHttpResponse implements Netty4RestResponse {

    private final int sequence;

    private final ChunkedRestResponseBody body;

    Netty4ChunkedHttpResponse(int sequence, HttpVersion version, RestStatus status, ChunkedRestResponseBody body) {
        super(version, HttpResponseStatus.valueOf(status.getStatus()));
        this.sequence = sequence;
        this.body = body;
    }

    public ChunkedRestResponseBody body() {
        return body;
    }

    @Override
    public int getSequence() {
        return sequence;
    }
}
