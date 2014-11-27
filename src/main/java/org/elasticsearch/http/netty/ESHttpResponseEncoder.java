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

package org.elasticsearch.http.netty;

import org.elasticsearch.common.netty.NettyUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.CompositeChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

import java.util.List;

/**
 * Wraps a netty {@link HttpResponseEncoder} and makes sure that if the resulting
 * channel buffer is composite, it will use the correct gathering flag. See more
 * at {@link NettyUtils#DEFAULT_GATHERING}.
 */
public class ESHttpResponseEncoder extends HttpResponseEncoder {

    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
        Object retVal = super.encode(ctx, channel, msg);
        if (retVal instanceof CompositeChannelBuffer) {
            CompositeChannelBuffer ccb = (CompositeChannelBuffer) retVal;
            if (ccb.useGathering() != NettyUtils.DEFAULT_GATHERING) {
                List<ChannelBuffer> decompose = ccb.decompose(ccb.readerIndex(), ccb.readableBytes());
                return ChannelBuffers.wrappedBuffer(NettyUtils.DEFAULT_GATHERING,
                        decompose.toArray(new ChannelBuffer[decompose.size()]));
            }
        }
        return retVal;
    }
}
