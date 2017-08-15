package org.elasticsearch.transport.nio.channel;

import org.elasticsearch.action.ActionListener;

import java.util.function.Consumer;

public class ChannelConsumerAdaptor implements ActionListener<NioChannel> {

    private final NioChannel channel;
    private final Consumer<NioChannel> consumer;

    private ChannelConsumerAdaptor(NioChannel channel, Consumer<NioChannel> consumer) {
        this.channel = channel;
        this.consumer = consumer;
    }

    static ChannelConsumerAdaptor adapt(NioChannel channel, Consumer<NioChannel> consumer) {
        return new ChannelConsumerAdaptor(channel, consumer);
    }

    @Override
    public void onResponse(NioChannel channel) {
        consumer.accept(channel);
    }

    @Override
    public void onFailure(Exception e) {
        consumer.accept(channel);
    }
}

