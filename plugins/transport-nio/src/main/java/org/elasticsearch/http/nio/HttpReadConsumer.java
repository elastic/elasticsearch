package org.elasticsearch.http.nio;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.SelectionKeyUtils;
import org.elasticsearch.nio.SocketChannelContext;

import java.nio.ByteBuffer;
import java.util.Queue;

public class HttpReadConsumer implements SocketChannelContext.ReadConsumer {

    private final NioSocketChannel channel;
    private final NettyChannelAdaptor nettyPipelineAdaptor;
    private final NioHttpRequestHandler requestHandler;

    public HttpReadConsumer(NioSocketChannel channel, NettyChannelAdaptor adaptor, NioHttpRequestHandler requestHandler) {
        this.channel = channel;
        this.requestHandler = requestHandler;
        this.nettyPipelineAdaptor = adaptor;
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) {
        boolean noPendingWritesPriorToDecode = !nettyPipelineAdaptor.hasMessages();

        ByteBuf inboundBytes = toByteBuf(channelBuffer);

        int readDelta = inboundBytes.readableBytes();
        Queue<Object> requests = nettyPipelineAdaptor.decode(inboundBytes);

        Object msg;
        while ((msg = requests.poll()) != null) {
            requestHandler.handleMessage(channel, nettyPipelineAdaptor, msg);
        }

        // TODO: I'm not sure this is currently safe with recycling
        return readDelta;
    }

    private static ByteBuf toByteBuf(InboundChannelBuffer channelBuffer) {
        ByteBuffer[] preIndexBuffers = channelBuffer.sliceBuffersFrom(channelBuffer.getIndex());
        if (preIndexBuffers.length == 1) {
            return Unpooled.wrappedBuffer(preIndexBuffers[0]);
        } else {
            CompositeByteBuf byteBuf = Unpooled.compositeBuffer(preIndexBuffers.length);
            for (ByteBuffer buffer : preIndexBuffers) {
                ByteBuf component = Unpooled.wrappedBuffer(buffer);
                byteBuf.addComponent(true, component);
            }
            return byteBuf;
        }
    }
}
