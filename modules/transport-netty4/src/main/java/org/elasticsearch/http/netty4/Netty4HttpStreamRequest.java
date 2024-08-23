/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.HttpRequest;

public final class Netty4HttpStreamRequest extends Netty4AbstractHttpRequest implements Netty4HttpRequest {

    private final Netty4HttpRequestBodyStream body;

    public Netty4HttpStreamRequest(int sequence, io.netty.handler.codec.http.HttpRequest request, Netty4HttpRequestBodyStream body) {
        super(sequence, request);
        this.body = body;
    }

    @Override
    public Netty4HttpRequestBodyStream body() {
        return body;
    }

    @Override
    public Exception getInboundException() {
        return null;
    }

    @Override
    public void release() {
        body.discard();
    }

    @Override
    public HttpRequest releaseAndCopy() {
        return this;
    }
}
