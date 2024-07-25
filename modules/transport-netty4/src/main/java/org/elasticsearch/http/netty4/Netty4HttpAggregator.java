/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;

import java.util.List;
import java.util.function.Predicate;

/**
 * A wrapper around {@link HttpObjectAggregator}, provides optional aggregation for {@link PipelinedHttpObject}'s.
 * A {@code decider} predicate selects HTTP requests that will be aggregated into {@link PipelinedFullHttpRequest}.
 */
public class Netty4HttpAggregator extends HttpObjectAggregator {

    private static final Predicate<PipelinedHttpRequest> ALWAYS_AGGREGATE = (req) -> true;

    private final Predicate<PipelinedHttpRequest> decider;
    private boolean shouldAggregate;

    public Netty4HttpAggregator(int maxContentLength) {
        this(maxContentLength, ALWAYS_AGGREGATE);
    }

    public Netty4HttpAggregator(int maxContentLength, Predicate<PipelinedHttpRequest> decider) {
        super(maxContentLength);
        this.decider = decider;
    }

    @Override
    public boolean acceptInboundMessage(Object msg) {
        if (msg instanceof PipelinedHttpRequest request) {
            shouldAggregate = decider.test(request);
            return shouldAggregate;
        } else if (msg instanceof PipelinedHttpContent || msg instanceof PipelinedLastHttpContent) {
            return shouldAggregate;
        } else {
            return false;
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
        super.decode(ctx, msg, out);
        out.replaceAll(o -> new PipelinedFullHttpRequest((FullHttpRequest) o, ((PipelinedHttpObject) msg).sequence()));
    }
}
