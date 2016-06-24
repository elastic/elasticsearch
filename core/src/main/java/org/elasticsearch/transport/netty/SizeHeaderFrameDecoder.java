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

package org.elasticsearch.transport.netty;

import org.elasticsearch.common.netty.NettyUtils;
import org.elasticsearch.transport.TCPHeader;
import org.elasticsearch.transport.TCPTransport;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;

/**
 */
public class SizeHeaderFrameDecoder extends FrameDecoder {

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        try {
            boolean continueProcessing = TCPTransport.validateMessageHeader(NettyUtils.toBytesReference(buffer));
            buffer.skipBytes(TCPHeader.MARKER_BYTES_SIZE + TCPHeader.MESSAGE_LENGTH_SIZE);
            return continueProcessing ? buffer : null;
        } catch (IllegalArgumentException ex) {
            throw new TooLongFrameException(ex.getMessage(), ex);
        } catch (IllegalStateException ex) {
            return null;
        }
    }

}
