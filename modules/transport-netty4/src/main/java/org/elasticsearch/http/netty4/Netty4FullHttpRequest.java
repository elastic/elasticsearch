/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.FullHttpRequest;

import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.transport.netty4.Netty4Utils;

public non-sealed class Netty4FullHttpRequest extends Netty4AbstractHttpRequest implements Netty4HttpRequest {

    private final FullHttpRequest request;
    private final HttpBody.Full body;

    public Netty4FullHttpRequest(int sequence, FullHttpRequest request) {
        super(sequence, request);
        this.request = request;
        this.body = HttpBody.fromReleasableBytesReference(Netty4Utils.toReleasableBytesReference(request.content()));
    }

    public Netty4FullHttpRequest(Netty4FullHttpRequest other) {
        super(other.sequence(), other.request);
        this.request = other.request;
        this.body = other.body;
    }

    @Override
    public HttpBody body() {
        return body;
    }

    @Override
    public HttpRequest removeHeader(String header) {
        super.removeHeader(header);
        return this;
    }

    @Override
    public Exception getInboundException() {
        return null;
    }

    @Override
    public void release() {
        body.close();
        request.release();
    }

    @Override
    public Netty4FullHttpRequest releaseAndCopy() {
        var copy = request.copy();
        release();
        return new Netty4FullHttpRequest(sequence(), copy);
    }
}
