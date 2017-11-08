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

package org.elasticsearch.transport.nio.http;

import io.netty.channel.ChannelPromise;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.transport.nio.ByteWriteOperation;
import org.elasticsearch.transport.nio.SocketSelector;
import org.elasticsearch.transport.nio.WriteOperation;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.WriteContext;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

public class HttpWriteContext implements WriteContext {

    private final NioSocketChannel channel;
    private final NettyChannelAdaptor adaptor;
    private ByteWriteOperation partiallyFlushed;

    public HttpWriteContext(NioSocketChannel channel, NettyChannelAdaptor adaptor) {
        this.channel = channel;
        this.adaptor = adaptor;
    }

    @Override
    public void sendMessage(Object message, ActionListener<NioChannel> listener) {
        if (channel.isWritable() == false) {
            listener.onFailure(new ClosedChannelException());
            return;
        }

        WriteOperation writeOperation = new HttpWriteOperation(channel, message, listener);
        SocketSelector selector = channel.getSelector();

        // If we are on the selector thread, we can queue the message directly in the channel buffer.
        // Otherwise we must call queueWrite which will dispatch to the selector thread.
        if (selector.isOnCurrentThread()) {
            // TODO: Eval if we will allow writes from sendMessage
            selector.queueWriteInChannelBuffer(writeOperation);
        } else {
            selector.queueWrite(writeOperation);
        }

    }

    @Override
    public void queueWriteOperations(WriteOperation writeOperation) {
        assert channel.getSelector().isOnCurrentThread() : "Must be on selector thread to queue writes";

        HttpWriteOperation httpWriteOperation = (HttpWriteOperation) writeOperation;
        NettyActionListener listener = (NettyActionListener) httpWriteOperation.getListener();
        adaptor.write(httpWriteOperation.getHttpResponse(), listener);
    }

    @Override
    public void flushChannel() throws IOException {
        assert channel.getSelector().isOnCurrentThread() : "Must be on selector thread to access queued writes";

        if (partiallyFlushed != null) {
            if (WriteContext.flushOperation(channel, partiallyFlushed)) {
                partiallyFlushed = null;
            } else {
                return;
            }
        }

        Tuple<BytesReference, ChannelPromise> message;
        boolean previousMessageFullyFlushed = true;
        while (previousMessageFullyFlushed && (message = adaptor.popMessage()) != null) {
            ChannelPromise promise = message.v2();
            NettyActionListener listener;
            if (promise instanceof NettyActionListener) {
                listener = (NettyActionListener) promise;
            } else {
                listener = new NettyActionListener(promise);
            }
            ByteWriteOperation writeOperation = new ByteWriteOperation(channel, message.v1(), listener);

            if (WriteContext.flushOperation(channel, writeOperation) == false) {
                partiallyFlushed = writeOperation;
                previousMessageFullyFlushed = false;
            }
        }
    }

    @Override
    public boolean hasQueuedWriteOps() {
        assert channel.getSelector().isOnCurrentThread() : "Must be on selector thread to access queued writes";
        return partiallyFlushed != null || adaptor.hasMessages();
    }

    @Override
    public void clearQueuedWriteOps(Exception e) {
        assert channel.getSelector().isOnCurrentThread() : "Must be on selector thread to access queued writes";

        // Right now there is an assumption that all resources will be released by the promise completion
        if (partiallyFlushed != null) {
            partiallyFlushed.getListener().onFailure(e);
        }

        Tuple<BytesReference, ChannelPromise> message;
        while ((message = adaptor.popMessage()) != null) {
            message.v2().setFailure(e);
            BytesReference bytes = message.v1();
        }

        adaptor.closeNettyChannel();
    }
}
