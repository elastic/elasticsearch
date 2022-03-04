/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.io.IOException;
import java.util.function.Consumer;

public class BytesChannelContext extends SocketChannelContext {

    public BytesChannelContext(
        NioSocketChannel channel,
        NioSelector selector,
        Config.Socket socketConfig,
        Consumer<Exception> exceptionHandler,
        NioChannelHandler handler,
        InboundChannelBuffer channelBuffer
    ) {
        super(channel, selector, socketConfig, exceptionHandler, handler, channelBuffer);
    }

    @Override
    public int read() throws IOException {
        int bytesRead = readFromChannel(channelBuffer);

        if (bytesRead == 0) {
            return 0;
        }

        handleReadBytes();

        return bytesRead;
    }

    @Override
    public void flushChannel() throws IOException {
        getSelector().assertOnSelectorThread();
        boolean lastOpCompleted = true;
        FlushOperation flushOperation;
        while (lastOpCompleted && (flushOperation = getPendingFlush()) != null) {
            try {
                if (singleFlush(flushOperation)) {
                    currentFlushOperationComplete();
                } else {
                    lastOpCompleted = false;
                }
            } catch (IOException e) {
                currentFlushOperationFailed(e);
                throw e;
            }
        }
    }

    @Override
    public void closeChannel() {
        if (isClosing.compareAndSet(false, true)) {
            getSelector().queueChannelClose(channel);
        }
    }

    @Override
    public boolean selectorShouldClose() {
        return closeNow() || isClosing.get();
    }

    /**
     * Returns a boolean indicating if the operation was fully flushed.
     */
    private boolean singleFlush(FlushOperation flushOperation) throws IOException {
        flushToChannel(flushOperation);
        return flushOperation.isFullyFlushed();
    }
}
