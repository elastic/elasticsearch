/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.LastHttpContent;

public class ValidatedHttpObjectAggregator extends HttpObjectAggregator {

    public ValidatedHttpObjectAggregator(int maxContentLength) {
        super(maxContentLength);
    }

    @Override
    protected FullHttpMessage beginAggregation(HttpMessage start, ByteBuf content) throws Exception {
        FullHttpMessage aggregatingMessage = super.beginAggregation(start, content);
        if (start instanceof ValidatedHttpRequest && aggregatingMessage instanceof FullHttpRequest) {
            return new ValidatedFullHttpRequest((FullHttpRequest) aggregatingMessage, ((ValidatedHttpRequest) start).validationResult());
        } else {
            return aggregatingMessage;
        }
    }

    @Override
    protected void aggregate(FullHttpMessage aggregated, HttpContent content) throws Exception {
        if (content instanceof LastHttpContent) {
            // Merge trailing headers into the message.
            if (aggregated instanceof ValidatedFullHttpRequest) {
                ((ValidatedFullHttpRequest) aggregated).setTrailingHeaders(((LastHttpContent) content).trailingHeaders());
            }
        }
    }
}
