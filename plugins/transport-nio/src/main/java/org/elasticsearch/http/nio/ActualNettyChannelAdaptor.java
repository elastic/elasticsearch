package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.nio.WriteOperation;

import java.util.function.BiConsumer;

public class ActualNettyChannelAdaptor implements AutoCloseable {

    private final EmbeddedChannel nettyChannel;

    public ActualNettyChannelAdaptor(ChannelHandler[] handlers) {
        nettyChannel = new EmbeddedChannel();
        nettyChannel.pipeline().addLast(handlers);
        nettyChannel.pipeline().addLast("write_captor", new ChannelOutboundHandlerAdapter() {

            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                // This is a little tricky. The embedded channel will complete the promise once it writes the message
                // to its outbound buffer. We do not want to complete the promise until the message is sent. So we
                // intercept the promise and pass a different promise back to the rest of the pipeline.

                ByteBuf message = (ByteBuf) msg;
                try {
                    promise.addListener((f) -> message.release());
//                    byteOps.add(new BytesWriteOperation(channelContext, message.nioBuffers(), new NettyActionListener(promise)));
                } catch (Exception e) {
                    promise.setFailure(e);
                }
            }
        });
    }

    public void addToPipeline(ChannelHandler handler) {
//        nettyChannel.pipeline().addBefore("write_captor", handler);
    }

    @Override
    public void close() throws Exception {
        // This should be safe as we are not a real network channel
        ChannelFuture closeFuture = nettyChannel.close();
        closeFuture.await();
        if (closeFuture.isSuccess() == false) {
            Throwable cause = closeFuture.cause();
            ExceptionsHelper.dieOnError(cause);
            throw (Exception) cause;
        }
    }

    public void addCloseListener(BiConsumer<Void, Exception> consumer) {
        nettyChannel.closeFuture().addListener(f -> {
            if (f.isSuccess()) {
                consumer.accept(null, null);
            } else {
                final Throwable cause = f.cause();
                ExceptionsHelper.dieOnError(cause);
                assert cause instanceof Exception;
                consumer.accept(null, (Exception) cause);
            }
        });
    }

    public void write(WriteOperation writeOperation) {
        nettyChannel.writeAndFlush(writeOperation.getObject(), (NettyActionListener) writeOperation.getListener());
    }
}
