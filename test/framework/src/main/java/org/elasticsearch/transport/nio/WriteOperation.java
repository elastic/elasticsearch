package org.elasticsearch.transport.nio;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.io.IOException;

public abstract class WriteOperation {

    protected final NioSocketChannel channel;
    protected final ActionListener<NioChannel> listener;

    public WriteOperation(NioSocketChannel channel, ActionListener<NioChannel> listener) {
        this.channel = channel;
        this.listener = listener;
    }


    public ActionListener<NioChannel> getListener() {
        return listener;
    }

    public NioSocketChannel getChannel() {
        return channel;
    }
}
