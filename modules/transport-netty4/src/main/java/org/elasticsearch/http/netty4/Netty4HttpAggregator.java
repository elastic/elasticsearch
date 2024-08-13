/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;

import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.netty4.internal.HttpHeadersAuthenticatorUtils;

import java.util.function.Predicate;

public class Netty4HttpAggregator extends HttpObjectAggregator {
    private static final Predicate<HttpPreRequest> IGNORE_TEST = (req) -> req.uri().startsWith("/_test/request-stream") == false;

    private final Predicate<HttpPreRequest> decider;
    private boolean shouldAggregate;

    public Netty4HttpAggregator(int maxContentLength) {
        this(maxContentLength, IGNORE_TEST);
    }

    public Netty4HttpAggregator(int maxContentLength, Predicate<HttpPreRequest> decider) {
        super(maxContentLength);
        this.decider = decider;
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        if (msg instanceof HttpRequest request) {
            var preReq = HttpHeadersAuthenticatorUtils.asHttpPreRequest(request);
            shouldAggregate = decider.test(preReq);
        }
        if (shouldAggregate) {
            return super.acceptInboundMessage(msg);
        } else {
            return false;
        }
    }
}
