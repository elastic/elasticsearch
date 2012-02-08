package org.elasticsearch.common.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

/**
 * A marker to not remove frame decoder from the resulting jar so plugins can use it.
 */
public class KeepFrameDecoder extends FrameDecoder {

    public static final KeepFrameDecoder decoder = new KeepFrameDecoder();

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        return null;
    }
}
