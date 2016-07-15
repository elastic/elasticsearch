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
import org.jboss.netty.channel.UpstreamMessageEvent;

import java.net.SocketAddress;

/**
 * Permits upstream message events to be ordered.
 *
 * @author Christopher Hunt
 */
public class OrderedUpstreamMessageEvent extends UpstreamMessageEvent {
    final int sequence;

    public OrderedUpstreamMessageEvent(final int sequence, final Channel channel, final Object msg, final SocketAddress remoteAddress) {
        super(channel, msg, remoteAddress);
        this.sequence = sequence;
    }

    public int getSequence() {
        return sequence;
    }

}
