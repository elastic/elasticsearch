/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport.netty3;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChannelUpstreamHandler;

import java.util.Set;

@ChannelHandler.Sharable
public class Netty3OpenChannelsHandler implements ChannelUpstreamHandler, Releasable {

    final Set<Channel> openChannels = ConcurrentCollections.newConcurrentSet();
    final CounterMetric openChannelsMetric = new CounterMetric();
    final CounterMetric totalChannelsMetric = new CounterMetric();

    final Logger logger;

    public Netty3OpenChannelsHandler(Logger logger) {
        this.logger = logger;
    }

    final ChannelFutureListener remover = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            boolean removed = openChannels.remove(future.getChannel());
            if (removed) {
                openChannelsMetric.dec();
            }
            if (logger.isTraceEnabled()) {
                logger.trace("channel closed: {}", future.getChannel());
            }
        }
    };

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof ChannelStateEvent) {
            ChannelStateEvent evt = (ChannelStateEvent) e;
            // OPEN is also sent to when closing channel, but with FALSE on it to indicate it closes
            if (evt.getState() == ChannelState.OPEN && Boolean.TRUE.equals(evt.getValue())) {
                if (logger.isTraceEnabled()) {
                    logger.trace("channel opened: {}", ctx.getChannel());
                }
                boolean added = openChannels.add(ctx.getChannel());
                if (added) {
                    openChannelsMetric.inc();
                    totalChannelsMetric.inc();
                    ctx.getChannel().getCloseFuture().addListener(remover);
                }
            }
        }
        ctx.sendUpstream(e);
    }

    public long numberOfOpenChannels() {
        return openChannelsMetric.count();
    }

    public long totalChannels() {
        return totalChannelsMetric.count();
    }

    @Override
    public void close() {
        for (Channel channel : openChannels) {
            channel.close().awaitUninterruptibly();
        }
    }
}
