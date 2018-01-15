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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class BytesChannelContext implements ChannelContext {

    private final NioSocketChannel channel;
    private final ReadConsumer readConsumer;
    private final InboundChannelBuffer channelBuffer;
    private final LinkedList<BytesWriteOperation> queued = new LinkedList<>();
    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private boolean peerClosed = false;
    private boolean ioException = false;

    public BytesChannelContext(NioSocketChannel channel, ReadConsumer readConsumer, InboundChannelBuffer channelBuffer) {
        this.channel = channel;
        this.readConsumer = readConsumer;
        this.channelBuffer = channelBuffer;
    }

    @Override
    public void channelRegistered() throws IOException {}

    @Override
    public int read() throws IOException {
        if (channelBuffer.getRemaining() == 0) {
            // Requiring one additional byte will ensure that a new page is allocated.
            channelBuffer.ensureCapacity(channelBuffer.getCapacity() + 1);
        }

        int bytesRead;
        try {
            bytesRead = channel.read(channelBuffer.sliceBuffersFrom(channelBuffer.getIndex()));
        } catch (IOException ex) {
            ioException = true;
            throw ex;
        }

        if (bytesRead == -1) {
            peerClosed = true;
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

        BytesWriteOperation writeOperation = new BytesWriteOperation(channel, buffers, listener);
        SocketSelector selector = channel.getSelector();
        if (selector.isOnCurrentThread() == false) {
            selector.queueWrite(writeOperation);
            return;
        }

        // TODO: Eval if we will allow writes from sendMessage
        selector.queueWriteInChannelBuffer(writeOperation);
    }

    @Override
    public void queueWriteOperation(WriteOperation writeOperation) {
        channel.getSelector().assertOnSelectorThread();
        queued.add((BytesWriteOperation) writeOperation);
    }

    @Override
    public void flushChannel() throws IOException {
        channel.getSelector().assertOnSelectorThread();
        int ops = queued.size();
        if (ops == 1) {
            singleFlush(queued.pop());
        } else if (ops > 1) {
            multiFlush();
        }
    }

    @Override
    public boolean hasQueuedWriteOps() {
        channel.getSelector().assertOnSelectorThread();
        return queued.isEmpty() == false;
    }

    @Override
    public void closeChannel() {
        if (isClosing.compareAndSet(false, true)) {
            channel.getSelector().queueChannelClose(channel);
        }
    }

    @Override
    public boolean selectorShouldClose() {
        return peerClosed || ioException || isClosing.get();
    }

    @Override
    public void closeFromSelector() {
        channel.getSelector().assertOnSelectorThread();
        // Set to true in order to reject new writes before queuing with selector
        isClosing.set(true);
        channelBuffer.close();
        for (BytesWriteOperation op : queued) {
            channel.getSelector().executeFailedListener(op.getListener(), new ClosedChannelException());
        }
        queued.clear();
    }

    private void singleFlush(BytesWriteOperation headOp) throws IOException {
        try {
            int written = channel.write(headOp.getBuffersToWrite());
            headOp.incrementIndex(written);
        } catch (IOException e) {
            channel.getSelector().executeFailedListener(headOp.getListener(), e);
            ioException = true;
            throw e;
        }

        if (headOp.isFullyFlushed()) {
            channel.getSelector().executeListener(headOp.getListener(), null);
        } else {
            queued.push(headOp);
        }
    }

    private void multiFlush() throws IOException {
        boolean lastOpCompleted = true;
        while (lastOpCompleted && queued.isEmpty() == false) {
            BytesWriteOperation op = queued.pop();
            singleFlush(op);
            lastOpCompleted = op.isFullyFlushed();
        }
    }
}
