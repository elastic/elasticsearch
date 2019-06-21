package org.elasticsearch.nio;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;

public abstract class DelegateHandler implements NioChannelHandler {

    private NioChannelHandler delegate;

    public DelegateHandler(NioChannelHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void channelRegistered() {
        this.delegate.channelRegistered();
    }

    @Override
    public WriteOperation createWriteOperation(SocketChannelContext context, Object message, BiConsumer<Void, Exception> listener) {
        return delegate.createWriteOperation(context, message, listener);
    }

    @Override
    public List<FlushOperation> writeToBytes(WriteOperation writeOperation) {
        return delegate.writeToBytes(writeOperation);
    }

    @Override
    public List<FlushOperation> pollFlushOperations() {
        return delegate.pollFlushOperations();
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
        return delegate.consumeReads(channelBuffer);
    }

    @Override
    public boolean closeNow() {
        return delegate.closeNow();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
