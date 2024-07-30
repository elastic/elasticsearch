/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpContent;
import org.elasticsearch.transport.netty4.Netty4Utils;

public class Netty4HttpRequestFullContent implements HttpContent.Full {

    private final io.netty.handler.codec.http.HttpContent httpContent;
    private final BytesReference bytes;

    public Netty4HttpRequestFullContent(io.netty.handler.codec.http.HttpContent httpContent) {
        this.httpContent = httpContent;
        this.bytes = Netty4Utils.toBytesReference(httpContent.content());
    }

    @Override
    public BytesReference bytes() {
        return bytes;
    }

    @Override
    public void release() {
        httpContent.release();
    }
}
