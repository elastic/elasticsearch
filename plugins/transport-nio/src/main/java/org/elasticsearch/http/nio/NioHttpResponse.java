/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.nio;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.RestStatus;

public class NioHttpResponse extends DefaultFullHttpResponse implements HttpResponse {

    private final HttpHeaders requestHeaders;

    NioHttpResponse(HttpHeaders requestHeaders, HttpVersion version, RestStatus status, BytesReference content) {
        super(version, HttpResponseStatus.valueOf(status.getStatus()), ByteBufUtils.toByteBuf(content));
        this.requestHeaders = requestHeaders;
    }

    @Override
    public void addHeader(String name, String value) {
        headers().add(name, value);
    }

    @Override
    public boolean containsHeader(String name) {
        return headers().contains(name);
    }

    public HttpHeaders requestHeaders() {
        return requestHeaders;
    }
}
