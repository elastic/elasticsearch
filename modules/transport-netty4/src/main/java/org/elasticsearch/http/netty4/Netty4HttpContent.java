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

public class Netty4HttpContent implements HttpContent {

    private final io.netty.handler.codec.http.HttpContent nettyContent;
    private final BytesReference ref;

    Netty4HttpContent(io.netty.handler.codec.http.HttpContent httpContent) {
        nettyContent = httpContent;
        ref = Netty4Utils.toBytesReference(httpContent.content());
    }

    @Override
    public BytesReference content() {
        return ref;
    }

    @Override
    public void release() {
        nettyContent.release();
    }
}
