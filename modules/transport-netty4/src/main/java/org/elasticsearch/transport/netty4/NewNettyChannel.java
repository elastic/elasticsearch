package org.elasticsearch.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.transport.NewTcpChannel;

public class NewNettyChannel implements NewTcpChannel<NewNettyChannel> {

    private final Channel channel;
    private final PlainListenableActionFuture<NewNettyChannel> future = PlainListenableActionFuture.newListenableFuture();

    NewNettyChannel(Channel channel) {
        this.channel = channel;
        this.channel.closeFuture().addListener(f -> {
            if (f.isSuccess()) {
                future.onResponse(this);
            } else {
                Throwable cause = f.cause();
                if (cause instanceof Error) {
                    Netty4Utils.maybeDie(cause);
                    future.onFailure(new Exception(cause));
                } else {
                    future.onFailure((Exception) cause);
                }
            }
        });
    }

    Channel getLowLevelChannel() {
        return channel;
    }

    @Override
    public ListenableActionFuture<NewNettyChannel> closeAsync() {
        channel.close();
        return future;
    }

    @Override
    public ListenableActionFuture<NewNettyChannel> getCloseFuture() {
        return future;
    }

    @Override
    public void setSoLinger(int value) {
        channel.config().setOption(ChannelOption.SO_LINGER, value);
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }
}
