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

package org.elasticsearch.transport.nio.channel;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.nio.InboundChannelBuffer;
import org.elasticsearch.transport.nio.SocketSelector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class NioSocketChannel extends AbstractNioChannel<SocketChannel> {

    private final InetSocketAddress remoteAddress;
    private final CompletableFuture<Void> connectContext = new CompletableFuture<>();
    private final SocketSelector socketSelector;
    private final AtomicBoolean contextsSet = new AtomicBoolean(false);
    private WriteContext writeContext;
    private ReadContext readContext;
    private BiConsumer<NioSocketChannel, Exception> exceptionContext;
    private Exception connectException;

    public NioSocketChannel(SocketChannel socketChannel, SocketSelector selector) throws IOException {
        super(socketChannel, selector);
        this.remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
        this.socketSelector = selector;
    }

    @Override
    public void closeFromSelector() throws IOException {
        assert socketSelector.isOnCurrentThread() : "Should only call from selector thread";
        // Even if the channel has already been closed we will clear any pending write operations just in case
        if (writeContext.hasQueuedWriteOps()) {
            writeContext.clearQueuedWriteOps(new ClosedChannelException());
        }
        readContext.close();

        super.closeFromSelector();
    }

    @Override
    public SocketSelector getSelector() {
        return socketSelector;
    }

    public int write(ByteBuffer[] buffers) throws IOException {
        if (buffers.length == 1) {
            return socketChannel.write(buffers[0]);
        } else {
            return (int) socketChannel.write(buffers);
        }
    }

    public int read(InboundChannelBuffer buffer) throws IOException {
        int bytesRead = (int) socketChannel.read(buffer.sliceBuffersFrom(buffer.getIndex()));

        if (bytesRead == -1) {
            return bytesRead;
        }

        buffer.incrementIndex(bytesRead);
        return bytesRead;
    }

    public void setContexts(ReadContext readContext, WriteContext writeContext, BiConsumer<NioSocketChannel, Exception> exceptionContext) {
        if (contextsSet.compareAndSet(false, true)) {
            this.readContext = readContext;
            this.writeContext = writeContext;
            this.exceptionContext = exceptionContext;
        } else {
            throw new IllegalStateException("Contexts on this channel were already set. They should only be once.");
        }
    }

    public WriteContext getWriteContext() {
        return writeContext;
    }

    public ReadContext getReadContext() {
        return readContext;
    }

    public BiConsumer<NioSocketChannel, Exception> getExceptionContext() {
        return exceptionContext;
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public boolean isConnectComplete() {
        return isConnectComplete0();
    }

    public boolean isWritable() {
        return isClosing.get() == false;
    }

    public boolean isReadable() {
        return isClosing.get() == false;
    }

    /**
     * This method will attempt to complete the connection process for this channel. It should be called for
     * new channels or for a channel that has produced a OP_CONNECT event. If this method returns true then
     * the connection is complete and the channel is ready for reads and writes. If it returns false, the
     * channel is not yet connected and this method should be called again when a OP_CONNECT event is
     * received.
     *
     * @return true if the connection process is complete
     * @throws IOException if an I/O error occurs
     */
    public boolean finishConnect() throws IOException {
        if (isConnectComplete0()) {
            return true;
        } else if (connectContext.isCompletedExceptionally()) {
            Exception exception = connectException;
            if (exception == null) {
                throw new AssertionError("Should have received connection exception");
            } else if (exception instanceof IOException) {
                throw (IOException) exception;
            } else {
                throw (RuntimeException) exception;
            }
        }

        boolean isConnected = socketChannel.isConnected();
        if (isConnected == false) {
            isConnected = internalFinish();
        }
        if (isConnected) {
            connectContext.complete(null);
        }
        return isConnected;
    }

    public void addConnectListener(ActionListener<Void> listener) {
        connectContext.whenComplete(ActionListener.toBiConsumer(listener));
    }

    @Override
    public String toString() {
        return "NioSocketChannel{" +
            "localAddress=" + getLocalAddress() +
            ", remoteAddress=" + remoteAddress +
            '}';
    }

    private boolean internalFinish() throws IOException {
        try {
            return socketChannel.finishConnect();
        } catch (IOException | RuntimeException e) {
            connectException = e;
            connectContext.completeExceptionally(e);
            throw e;
        }
    }

    private boolean isConnectComplete0() {
        return connectContext.isDone() && connectContext.isCompletedExceptionally() == false;
    }
}
