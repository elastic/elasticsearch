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

import org.elasticsearch.transport.nio.NetworkBytes;
import org.elasticsearch.transport.nio.SocketSelector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

public class NioSocketChannel extends AbstractNioChannel<SocketChannel> {

    private final InetSocketAddress remoteAddress;
    private final ConnectFuture connectFuture = new ConnectFuture();
    private final SocketSelector socketSelector;
    private WriteContext writeContext;
    private ReadContext readContext;

    public NioSocketChannel(String profile, SocketChannel socketChannel, SocketSelector selector) throws IOException {
        super(profile, socketChannel, selector);
        this.remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
        this.socketSelector = selector;
    }

    @Override
    public void closeFromSelector() {
        assert socketSelector.isOnCurrentThread() : "Should only call from selector thread";
        // Even if the channel has already been closed we will clear any pending write operations just in case
        if (writeContext.hasQueuedWriteOps()) {
            writeContext.clearQueuedWriteOps(new ClosedChannelException());
        }
        // TODO: Look at testing this line
        readContext.close();

        super.closeFromSelector();
    }

    @Override
    public SocketSelector getSelector() {
        return socketSelector;
    }

    public int write(NetworkBytes bytes) throws IOException {
        int written;
        if (bytes.isComposite() == false) {
            written = socketChannel.write(bytes.postIndexByteBuffer());
        } else {
            written = (int) socketChannel.write(bytes.postIndexByteBuffers());
        }
        if (written <= 0) {
            return written;
        }

        bytes.incrementIndex(written);

        return written;
    }

    public int read(NetworkBytes bytes) throws IOException {
        int bytesRead;
        if (bytes.isComposite()) {
            bytesRead = (int) socketChannel.read(bytes.postIndexByteBuffers());
        } else {
            bytesRead = socketChannel.read(bytes.postIndexByteBuffer());

        }

        if (bytesRead == -1) {
            return bytesRead;
        }

        bytes.incrementIndex(bytesRead);
        return bytesRead;
    }

    public void setContexts(ReadContext readContext, WriteContext writeContext) {
        this.readContext = readContext;
        this.writeContext = writeContext;
    }

    public WriteContext getWriteContext() {
        return writeContext;
    }

    public ReadContext getReadContext() {
        return readContext;
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public boolean isConnectComplete() {
        return connectFuture.isConnectComplete();
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
        if (connectFuture.isConnectComplete()) {
            return true;
        } else if (connectFuture.connectFailed()) {
            Exception exception = connectFuture.getException();
            if (exception instanceof IOException) {
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
            connectFuture.setConnectionComplete(this);
        }
        return isConnected;
    }

    public ConnectFuture getConnectFuture() {
        return connectFuture;
    }

    @Override
    public String toString() {
        return "NioSocketChannel{" +
            "profile=" + getProfile() +
            ", localAddress=" + getLocalAddress() +
            ", remoteAddress=" + remoteAddress +
            '}';
    }

    private boolean internalFinish() throws IOException {
        try {
            return socketChannel.finishConnect();
        } catch (IOException e) {
            connectFuture.setConnectionFailed(e);
            throw e;
        } catch (RuntimeException e) {
            connectFuture.setConnectionFailed(e);
            throw e;
        }
    }
}
