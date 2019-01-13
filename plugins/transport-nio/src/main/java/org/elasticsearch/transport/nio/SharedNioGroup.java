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

package org.elasticsearch.transport.nio;

import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.EventHandler;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.NioServerSocketChannel;
import org.elasticsearch.nio.NioSocketChannel;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

public class SharedNioGroup implements AutoCloseable {

    private final RefCountedNioGroup refCountedNioGroup;

    private final AtomicBoolean isOpen = new AtomicBoolean(true);

    SharedNioGroup(ThreadFactory threadFactory, int selectorCount, Function<Supplier<NioSelector>, EventHandler> eventHandlerFunction)
        throws IOException {
        this(new RefCountedNioGroup(new NioGroup(threadFactory, selectorCount, eventHandlerFunction)));
    }

    private SharedNioGroup(RefCountedNioGroup refCountedNioGroup) {
        this.refCountedNioGroup = refCountedNioGroup;
    }

    public <S extends NioServerSocketChannel> S bindServerChannel(InetSocketAddress address, ChannelFactory<S, ?> factory)
        throws IOException {
        ensureOpen();
        return refCountedNioGroup.nioGroup.bindServerChannel(address, factory);
    }

    public <S extends NioSocketChannel> S openChannel(InetSocketAddress address, ChannelFactory<?, S> factory) throws IOException {
        ensureOpen();
        return refCountedNioGroup.nioGroup.openChannel(address, factory);
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            refCountedNioGroup.decRef();
        }
    }

    /**
     * Returns a {@link SharedNioGroup} that shares the underlying {@link NioGroup}. Each shard group must be
     * closed or the underlying group will continue operate.
     *
     * @return shard group
     */
    SharedNioGroup getSharedInstance() {
        refCountedNioGroup.incRef();
        return new SharedNioGroup(refCountedNioGroup);
    }

    // Visible for testing
    boolean shareSameGroup(SharedNioGroup other) {
        return other.refCountedNioGroup == refCountedNioGroup;
    }

    private void ensureOpen() {
        if (isOpen.get() == false) {
            throw new IllegalStateException("SharedNioGroup is closed.");
        }
    }

    private static class RefCountedNioGroup extends AbstractRefCounted {

        private final NioGroup nioGroup;

        private RefCountedNioGroup(NioGroup nioGroup) {
            super("shareable-nio-group");
            this.nioGroup = nioGroup;
        }

        @Override
        protected void closeInternal() {
            try {
                nioGroup.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
