package org.elasticsearch.nio;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class SupplierWriteOperation implements WriteOperation {


    private final BiConsumer<Void, Exception> listener;
    private final SocketChannelContext channel;
    private final Supplier<ByteBuffer[]> messageSupplier;

    public SupplierWriteOperation(SocketChannelContext channel, Supplier<ByteBuffer[]> messageSupplier,
                                  BiConsumer<Void, Exception> listener) {
        this.listener = listener;
        this.channel = channel;
        this.messageSupplier = messageSupplier;
    }

    @Override
    public BiConsumer<Void, Exception> getListener() {
        return listener;
    }

    @Override
    public SocketChannelContext getChannel() {
        return channel;
    }

    @Override
    public Supplier<ByteBuffer[]> getObject() {
        return messageSupplier;
    }
}
