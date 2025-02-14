/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestStatus;

/**
 * A http response that will be transferred via chunked encoding when handled by {@link Netty4HttpPipeliningHandler}.
 */
final class Netty4ChunkedHttpResponse extends DefaultHttpResponse implements Netty4HttpResponse, HttpResponse {

    private final int sequence;

    private final ChunkedRestResponseBodyPart firstBodyPart;

    Netty4ChunkedHttpResponse(int sequence, HttpVersion version, RestStatus status, ChunkedRestResponseBodyPart firstBodyPart) {
        super(version, HttpResponseStatus.valueOf(status.getStatus()));
        this.sequence = sequence;
        this.firstBodyPart = firstBodyPart;
    }

    public ChunkedRestResponseBodyPart firstBodyPart() {
        return firstBodyPart;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    @Override
    public void addHeader(String name, String value) {
        headers().add(name, value);
    }

    @Override
    public boolean containsHeader(String name) {
        return headers().contains(name);
    }
}
