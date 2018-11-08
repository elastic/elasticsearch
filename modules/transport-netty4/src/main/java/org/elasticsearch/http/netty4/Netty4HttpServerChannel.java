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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.concurrent.CompletableContext;
import org.elasticsearch.http.HttpServerChannel;

import java.net.InetSocketAddress;

public class Netty4HttpServerChannel implements HttpServerChannel {

    private final Channel channel;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();

    Netty4HttpServerChannel(Channel channel) {
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
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
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

    @Override
    public String toString() {
        return "Netty4HttpChannel{localAddress=" + getLocalAddress() + "}";
    }
}
