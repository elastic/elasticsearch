/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.netty4.Netty4Utils;

public class Netty4HttpResponse extends DefaultFullHttpResponse implements Netty4RestResponse {

    private final int sequence;

    Netty4HttpResponse(int sequence, HttpVersion version, RestStatus status, BytesReference content) {
        super(version, HttpResponseStatus.valueOf(status.getStatus()), Netty4Utils.toByteBuf(content));
        this.sequence = sequence;
    }

    @Override
    public int getSequence() {
        return sequence;
    }
}
