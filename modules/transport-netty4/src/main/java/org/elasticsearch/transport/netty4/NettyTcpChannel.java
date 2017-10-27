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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.transport.TcpChannel;

public class NettyTcpChannel implements TcpChannel<NettyTcpChannel> {

    private final Channel channel;
    private final PlainListenableActionFuture<NettyTcpChannel> future = PlainListenableActionFuture.newListenableFuture();

    NettyTcpChannel(Channel channel) {
        this.channel = channel;
        this.channel.closeFuture().addListener(f -> {
            if (f.isSuccess()) {
                future.onResponse(this);
            } else {
                Throwable cause = f.cause();
                if (cause instanceof Error) {
                    Netty4Utils.maybeDie(cause);
                    future.onFailure(new Exception(cause));
                } else {
                    future.onFailure((Exception) cause);
                }
            }
        });
    }

    Channel getLowLevelChannel() {
        return channel;
    }

    @Override
    public ListenableActionFuture<NettyTcpChannel> closeAsync() {
        channel.close();
        return future;
    }

    @Override
    public void addCloseListener(ActionListener<NettyTcpChannel> listener) {
        future.addListener(listener);
    }

    @Override
    public void setSoLinger(int value) {
        channel.config().setOption(ChannelOption.SO_LINGER, value);
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }
}
