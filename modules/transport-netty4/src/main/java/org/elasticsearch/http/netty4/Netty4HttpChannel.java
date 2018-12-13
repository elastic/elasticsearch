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

package org.elasticsearch.http.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.concurrent.CompletableContext;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpResponse;

import java.net.InetSocketAddress;

public class Netty4HttpChannel implements HttpChannel {

    private final Channel channel;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();

    Netty4HttpChannel(Channel channel) {
        this.channel = channel;
        this.channel.closeFuture().addListener(f -> {
            if (f.isSuccess()) {
                closeContext.complete(null);
            } else {
                Throwable cause = f.cause();
                if (cause instanceof Error) {
                    ExceptionsHelper.maybeDieOnAnotherThread(cause);
                    closeContext.completeExceptionally(new Exception(cause));
                } else {
                    closeContext.completeExceptionally((Exception) cause);
                }
            }
        });
    }

    @Override
    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
        ChannelPromise writePromise = channel.newPromise();
        writePromise.addListener(f -> {
            if (f.isSuccess()) {
                listener.onResponse(null);
            } else {
                final Throwable cause = f.cause();
                ExceptionsHelper.maybeDieOnAnotherThread(cause);
                if (cause instanceof Error) {
                    listener.onFailure(new Exception(cause));
                } else {
                    listener.onFailure((Exception) cause);
                }
            }
        });
        channel.writeAndFlush(response, writePromise);
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() {
        channel.close();
    }

    public Channel getNettyChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return "Netty4HttpChannel{" +
            "localAddress=" + getLocalAddress() +
            ", remoteAddress=" + getRemoteAddress() +
            '}';
    }
}
