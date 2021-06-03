/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;

public abstract class DelegatingHandler implements NioChannelHandler {

    private NioChannelHandler delegate;

    public DelegatingHandler(NioChannelHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void channelActive() {
        this.delegate.channelActive();
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
