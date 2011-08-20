/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.netty;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.netty.channel.Channel;
import org.elasticsearch.common.netty.channel.ChannelEvent;
import org.elasticsearch.common.netty.channel.ChannelFuture;
import org.elasticsearch.common.netty.channel.ChannelFutureListener;
import org.elasticsearch.common.netty.channel.ChannelHandler;
import org.elasticsearch.common.netty.channel.ChannelHandlerContext;
import org.elasticsearch.common.netty.channel.ChannelState;
import org.elasticsearch.common.netty.channel.ChannelStateEvent;
import org.elasticsearch.common.netty.channel.ChannelUpstreamHandler;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Set;

/**
 * @author kimchy (shay.banon)
 */
@ChannelHandler.Sharable
public class OpenChannelsHandler implements ChannelUpstreamHandler {

    private Set<Channel> openChannels = ConcurrentCollections.newConcurrentSet();
    private CounterMetric openChannelsMetric = new CounterMetric();

    private final ChannelFutureListener remover = new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) throws Exception {
            boolean removed = openChannels.remove(future.getChannel());
            if (removed) {
                openChannelsMetric.dec();
            }
        }
    };

    @Override public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof ChannelStateEvent) {
            ChannelStateEvent evt = (ChannelStateEvent) e;
            if (evt.getState() == ChannelState.OPEN) {
                boolean added = openChannels.add(ctx.getChannel());
                if (added) {
                    openChannelsMetric.inc();
                    ctx.getChannel().getCloseFuture().addListener(remover);
                }
            }
        }
        ctx.sendUpstream(e);
    }

    public long numberOfOpenChannels() {
        return openChannelsMetric.count();
    }

    public void close() {
        for (Channel channel : openChannels) {
            channel.close().awaitUninterruptibly();
        }
    }
}
