/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.FullHttpRequest;

import org.elasticsearch.http.HttpRequest;

public final class Netty4HttpRequestException extends Netty4FullHttpRequest implements Netty4HttpRequest {

    private final Exception exception;

    public Netty4HttpRequestException(int sequence, FullHttpRequest request, Exception exception) {
        super(sequence, request);
        this.exception = exception;
    }

    Netty4HttpRequestException(Netty4FullHttpRequest request, Exception exception) {
        super(request);
        this.exception = exception;
    }

    @Override
    public HttpRequest removeHeader(String header) {
        super.removeHeader(header);
        return this;
    }

    @Override
    public Exception getInboundException() {
        return exception;
    }

    @Override
    public Netty4HttpRequestException releaseAndCopy() {
        var copy = super.releaseAndCopy();
        return new Netty4HttpRequestException(copy, exception);
    }
}
