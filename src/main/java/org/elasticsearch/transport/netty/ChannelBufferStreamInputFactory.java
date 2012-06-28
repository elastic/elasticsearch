package org.elasticsearch.transport.netty;

import org.elasticsearch.common.io.stream.StreamInput;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 */
public class ChannelBufferStreamInputFactory {

    public static StreamInput create(ChannelBuffer buffer) {
        return new ChannelBufferStreamInput(buffer, buffer.readableBytes());
    }

    public static StreamInput create(ChannelBuffer buffer, int size) {
        return new ChannelBufferStreamInput(buffer, size);
    }
}
