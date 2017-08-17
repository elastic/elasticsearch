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

package org.elasticsearch.http.nio;

import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.transport.nio.ByteWriteOperation;
import org.elasticsearch.transport.nio.SocketSelector;
import org.elasticsearch.transport.nio.WriteOperation;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.WriteContext;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;

public class HttpWriteContext implements WriteContext {

    private final NioSocketChannel channel;
    private final ESEmbeddedChannel adaptor;
    private ByteWriteOperation partiallyFlushed;

    public HttpWriteContext(NioSocketChannel channel, ESEmbeddedChannel adaptor) {
        this.channel = channel;
        this.adaptor = adaptor;
    }

    @Override
    public void sendMessage(Object message, ActionListener<NioChannel> listener) {
        if (channel.isWritable() == false) {
            listener.onFailure(new ClosedChannelException());
            return;
        }
        HttpResponse response = (HttpResponse) message;

        WriteOperation writeOperation = new HttpWriteOperation(channel, response, listener);
        SocketSelector selector = channel.getSelector();
        if (selector.isOnCurrentThread() == false) {
            selector.queueWrite(writeOperation);
            return;
        }

        // TODO: Eval if we will allow writes from sendMessage
        selector.queueWriteInChannelBuffer(writeOperation);
    }

    @Override
    public void queueWriteOperations(WriteOperation writeOperation) {
        assert channel.getSelector().isOnCurrentThread() : "Must be on selector thread to queue writes";

        HttpWriteOperation httpWriteOperation = (HttpWriteOperation) writeOperation;
        ESChannelPromise listener = (ESChannelPromise) httpWriteOperation.getListener();
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
            ESChannelPromise listener;
            if (promise instanceof ESChannelPromise) {
                listener = (ESChannelPromise) promise;
            } else {
                listener = new ESChannelPromise(promise);
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
