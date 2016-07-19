package org.elasticsearch.http.netty3.pipelining;

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

// this file is from netty-http-pipelining, under apache 2.0 license
// see github.com/typesafehub/netty-http-pipelining

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DownstreamMessageEvent;

/**
 * Permits downstream channel events to be ordered and signalled as to whether more are to come for a given sequence.
 *
 * @author Christopher Hunt
 */
public class OrderedDownstreamChannelEvent implements ChannelEvent {

    final ChannelEvent ce;
    final OrderedUpstreamMessageEvent oue;
    final int subsequence;
    final boolean last;

    /**
     * Construct a downstream channel event for all types of events.
     *
     * @param oue         the OrderedUpstreamMessageEvent that this response is associated with
     * @param subsequence the sequence within the sequence
     * @param last        when set to true this indicates that there are no more responses to be received for the
     *                    original OrderedUpstreamMessageEvent
     */
    public OrderedDownstreamChannelEvent(final OrderedUpstreamMessageEvent oue, final int subsequence, boolean last,
                                         final ChannelEvent ce) {
        this.oue = oue;
        this.ce = ce;
        this.subsequence = subsequence;
        this.last = last;
    }

    /**
     * Convenience constructor signifying that this downstream message event is the last one for the given sequence,
     * and that there is only one response.
     */
    public OrderedDownstreamChannelEvent(final OrderedUpstreamMessageEvent oe,
                                         final Object message) {
        this(oe, 0, true, message);
    }

    /**
     * Convenience constructor for passing message events.
     */
    public OrderedDownstreamChannelEvent(final OrderedUpstreamMessageEvent oue, final int subsequence, boolean last,
                                         final Object message) {
        this(oue, subsequence, last, new DownstreamMessageEvent(oue.getChannel(), Channels.future(oue.getChannel()),
                message, oue.getRemoteAddress()));

    }

    public OrderedUpstreamMessageEvent getOrderedUpstreamMessageEvent() {
        return oue;
    }

    public int getSubsequence() {
        return subsequence;
    }

    public boolean isLast() {
        return last;
    }

    @Override
    public Channel getChannel() {
        return ce.getChannel();
    }

    @Override
    public ChannelFuture getFuture() {
        return ce.getFuture();
    }

    public ChannelEvent getChannelEvent() {
        return ce;
    }
}
