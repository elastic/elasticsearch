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

import org.elasticsearch.transport.nio.ESSelector;
import org.elasticsearch.transport.nio.SocketSelector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

public class NioSocketChannel extends AbstractNioChannel<SocketChannel> {

    private final InetSocketAddress remoteAddress;
    private final ConnectFuture connectFuture = new ConnectFuture();
    private WriteContext writeContext;
    private ReadContext readContext;
    private SocketSelector socketSelector;

    public NioSocketChannel(String profile, SocketChannel socketChannel) throws IOException {
        super(profile, socketChannel);
        this.remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
    }

    @Override
    public CloseFuture close(boolean attemptToCloseImmediately) {
        // Even if the channel has already been closed we will clear any pending write operations just in case
        if (attemptToCloseImmediately && state.get() > UNREGISTERED && selector.isOnCurrentThread() && writeContext.hasQueuedWriteOps()) {
            writeContext.clearQueuedWriteOps(new ClosedChannelException());
        }

        return super.close(attemptToCloseImmediately);
    }

    @Override
    public SocketSelector getSelector() {
        return socketSelector;
    }

    @Override
    public boolean markRegistered(ESSelector selector) {
        this.socketSelector = (SocketSelector) selector;
        return super.markRegistered(selector);
    }

    public int write(ByteBuffer buffer) throws IOException {
        return socketChannel.write(buffer);
    }

    public long vectorizedWrite(ByteBuffer[] buffers) throws IOException {
        return socketChannel.write(buffers);
    }

    public int read(ByteBuffer buffer) throws IOException {
        return socketChannel.read(buffer);
    }
    public WriteContext getWriteContext() {
        return writeContext;
    }

    public void setReadContext(TcpReadContext readContext) {
        this.readContext = readContext;
    }

    public ReadContext getReadContext() {
        return readContext;
    }

    public void setWriteContext(TcpWriteContext writeContext) {
        this.writeContext = writeContext;
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public boolean isConnectComplete() {
        return connectFuture.isConnectComplete();
    }

    public boolean isWritable() {
        return state.get() == REGISTERED;
    }

    public boolean isReadable() {
        return state.get() == REGISTERED;
    }

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

    public ConnectFuture getConnectFuture() throws IOException {
        return connectFuture;
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
