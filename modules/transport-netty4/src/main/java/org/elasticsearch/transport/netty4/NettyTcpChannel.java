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

package org.elasticsearch.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TransportException;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.util.concurrent.CompletableFuture;

public class NettyTcpChannel implements TcpChannel {

    private final Channel channel;
    private final CompletableFuture<Void> closeContext = new CompletableFuture<>();

    NettyTcpChannel(Channel channel) {
        this.channel = channel;
        this.channel.closeFuture().addListener(f -> {
            if (f.isSuccess()) {
                closeContext.complete(null);
            } else {
                Throwable cause = f.cause();
                if (cause instanceof Error) {
                    Netty4Utils.maybeDie(cause);
                    closeContext.completeExceptionally(cause);
                } else {
                    closeContext.completeExceptionally(cause);
                }
            }
        });
    }

    @Override
    public void close() {
        channel.close();
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.whenComplete(ActionListener.toBiConsumer(listener));
    }

    @Override
    public void setSoLinger(int value) {
        channel.config().setOption(ChannelOption.SO_LINGER, value);
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        ChannelPromise writePromise = channel.newPromise();
        writePromise.addListener(f -> {
            if (f.isSuccess()) {
                listener.onResponse(null);
            } else {
                final Throwable cause = f.cause();
                Netty4Utils.maybeDie(cause);
                assert cause instanceof Exception;
                listener.onFailure((Exception) cause);
            }
        });
        channel.writeAndFlush(Netty4Utils.toByteBuf(reference), writePromise);
        
        if (channel.eventLoop().isShutdown()) {
            listener.onFailure(new TransportException("Cannot send message, event loop is shutting down."));
        }
    }

    public Channel getLowLevelChannel() {
        return channel;
    }
}
