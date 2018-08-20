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

import java.net.InetSocketAddress;
import java.nio.channels.NetworkChannel;
import java.util.function.BiConsumer;

/**
 * This is a basic channel abstraction used by the {@link NioSelector}.
 * <p>
 * A channel is open once it is constructed. The channel remains open and {@link #isOpen()} will return
 * true until the channel is explicitly closed.
 */
public abstract class NioChannel {

    public boolean isOpen() {
        return getContext().isOpen();
    }

    /**
     * Adds a close listener to the channel. Multiple close listeners can be added. There is no guarantee
     * about the order in which close listeners will be executed. If the channel is already closed, the
     * listener is executed immediately.
     *
     * @param listener to be called at close
     */
    public void addCloseListener(BiConsumer<Void, Exception> listener) {
        getContext().addCloseListener(listener);
    }

    /**
     * Schedules channel for close. This process is asynchronous.
     */
    public void close() {
        getContext().closeChannel();
    }

    public abstract InetSocketAddress getLocalAddress();

    public abstract NetworkChannel getRawChannel();

    public abstract ChannelContext<?> getContext();
}
