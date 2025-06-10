/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.util.concurrent.FutureUtils;

import java.util.concurrent.TimeUnit;

/**
 * When channel auto-read is disabled handlers are responsible to read from channel.
 * But it's hard to detect when read is missing. This helper class print warnings
 * when no reads where detected in given time interval. Normally, in tests, 10 seconds is enough
 * to avoid test hang for too long, but can be increased if needed.
 */
class MissingReadDetector extends ChannelDuplexHandler {

    private static final Logger logger = LogManager.getLogger(MissingReadDetector.class);

    private final long interval;
    private final TimeProvider timer;
    private boolean pendingRead;
    private long lastRead;
    private ScheduledFuture<?> checker;

    MissingReadDetector(TimeProvider timer, long missingReadIntervalMillis) {
        this.interval = missingReadIntervalMillis;
        this.timer = timer;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        checker = ctx.channel().eventLoop().scheduleAtFixedRate(() -> {
            if (pendingRead == false) {
                long now = timer.absoluteTimeInMillis();
                if (now >= lastRead + interval) {
                    logger.warn("chan-id={} haven't read from channel for [{}ms]", ctx.channel().id(), (now - lastRead));
                }
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
        super.handlerAdded(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        if (checker != null) {
            FutureUtils.cancel(checker);
        }
        super.handlerRemoved(ctx);
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        pendingRead = true;
        ctx.read();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert ctx.channel().config().isAutoRead() == false : "auto-read must be always disabled";
        pendingRead = false;
        lastRead = timer.absoluteTimeInMillis();
        ctx.fireChannelRead(msg);
    }
}
