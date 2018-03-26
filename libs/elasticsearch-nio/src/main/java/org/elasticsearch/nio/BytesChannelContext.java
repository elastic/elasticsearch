/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.nio;

import org.elasticsearch.transport.nio.BytesFlushProducer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class BytesChannelContext extends SocketChannelContext {

    private final ReadConsumer readConsumer;
    private final FlushProducer writeProducer;
    private final InboundChannelBuffer channelBuffer;
    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private FlushOperation currentFlushOperation;

    public BytesChannelContext(NioSocketChannel channel, SocketSelector selector, Consumer<Exception> exceptionHandler,
                               ReadConsumer readConsumer, InboundChannelBuffer channelBuffer) {
        this(channel, selector, exceptionHandler, readConsumer, new BytesFlushProducer(selector), channelBuffer);
    }

    public BytesChannelContext(NioSocketChannel channel, SocketSelector selector, Consumer<Exception> exceptionHandler,
                               ReadConsumer readConsumer, FlushProducer writeProducer, InboundChannelBuffer channelBuffer) {
        super(channel, selector, exceptionHandler);
        this.readConsumer = readConsumer;
        this.writeProducer = writeProducer;
        this.channelBuffer = channelBuffer;
    }

    @Override
    public int read() throws IOException {
        if (channelBuffer.getRemaining() == 0) {
            // Requiring one additional byte will ensure that a new page is allocated.
            channelBuffer.ensureCapacity(channelBuffer.getCapacity() + 1);
        }

        int bytesRead = readFromChannel(channelBuffer.sliceBuffersFrom(channelBuffer.getIndex()));

        if (bytesRead == 0) {
            return 0;
        }

        channelBuffer.incrementIndex(bytesRead);

        int bytesConsumed = Integer.MAX_VALUE;
        while (bytesConsumed > 0 && channelBuffer.getIndex() > 0) {
            bytesConsumed = readConsumer.consumeReads(channelBuffer);
            channelBuffer.release(bytesConsumed);
        }

        return bytesRead;
    }

    @Override
    public void sendMessage(ByteBuffer[] buffers, BiConsumer<Void, Throwable> listener) {
        if (isClosing.get()) {
            listener.accept(null, new ClosedChannelException());
            return;
        }

        WriteOperation writeOperation = new WriteOperation(this, buffers, listener);
        SocketSelector selector = getSelector();
        if (selector.isOnCurrentThread() == false) {
            selector.queueWrite(writeOperation);
            return;
        }

        selector.queueWriteInChannelBuffer(writeOperation);
    }

    @Override
    public void queueWriteOperation(WriteOperation writeOperation) {
        getSelector().assertOnSelectorThread();
        writeProducer.produceWrites(writeOperation);
        if (currentFlushOperation == null) {
            currentFlushOperation = writeProducer.pollFlushOperation();
        }
    }

    @Override
    public void flushChannel() throws IOException {
        getSelector().assertOnSelectorThread();
        boolean lastOpCompleted = true;
        while (lastOpCompleted && currentFlushOperation != null) {
            try {
                if (singleFlush(currentFlushOperation)) {
                    lastOpCompleted = true;
                    currentFlushOperation = writeProducer.pollFlushOperation();
                } else {
                    lastOpCompleted = false;
                }
            } catch (IOException e) {
                currentFlushOperation = writeProducer.pollFlushOperation();
                throw e;
            }
        }
    }

    @Override
    public boolean hasQueuedWriteOps() {
        getSelector().assertOnSelectorThread();
        return currentFlushOperation != null;
    }

    @Override
    public void closeChannel() {
        if (isClosing.compareAndSet(false, true)) {
            getSelector().queueChannelClose(channel);
        }
    }

    @Override
    public boolean selectorShouldClose() {
        return isPeerClosed() || hasIOException() || isClosing.get();
    }

    @Override
    public void closeFromSelector() throws IOException {
        getSelector().assertOnSelectorThread();
        if (channel.isOpen()) {
            IOException channelCloseException = null;
            try {
                super.closeFromSelector();
            } catch (IOException e) {
                channelCloseException = e;
            }
            // Set to true in order to reject new writes before queuing with selector
            isClosing.set(true);
            channelBuffer.close();
            if (currentFlushOperation != null) {
                getSelector().executeFailedListener(currentFlushOperation.getListener(), new ClosedChannelException());
                currentFlushOperation = null;
            }
            try {
                writeProducer.close();
            } catch (IOException e) {
                if (channelCloseException == null) {
                    channelCloseException = e;
                } else {
                    channelCloseException.addSuppressed(e);
                }
            }

            if (channelCloseException != null) {
                throw channelCloseException;
            }
        }
    }

    /**
     * Returns a boolean indicating if the operation was fully flushed.
     */
    private boolean singleFlush(FlushOperation headOp) throws IOException {
        try {
            int written = flushToChannel(headOp.getBuffersToWrite());
            headOp.incrementIndex(written);
        } catch (IOException e) {
            getSelector().executeFailedListener(headOp.getListener(), e);
            throw e;
        }

        boolean fullyFlushed = headOp.isFullyFlushed();
        if (fullyFlushed) {
            getSelector().executeListener(headOp.getListener(), null);
        }
        return fullyFlushed;
    }
}
