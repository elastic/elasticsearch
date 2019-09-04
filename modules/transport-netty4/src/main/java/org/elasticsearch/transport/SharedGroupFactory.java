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

package org.elasticsearch.transport;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.transport.netty4.Netty4Transport;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * Creates and returns {@link io.netty.channel.EventLoopGroup} instances. It will return a shared group for
 * both {@link #getHttpGroup()} and {@link #getTransportGroup()} if
 * {@link org.elasticsearch.http.netty4.Netty4HttpServerTransport#SETTING_HTTP_WORKER_COUNT} is configured to be 0.
 * If that setting is not 0, then it will return a different group in the {@link #getHttpGroup()} call.
 */
public final class SharedGroupFactory {

    private final Settings settings;
    private final int workerCount;
    private final int httpWorkerCount;

    private RefCountedGroup refCountedGroup;

    public SharedGroupFactory(Settings settings) {
        this.settings = settings;
        this.workerCount = Netty4Transport.WORKER_COUNT.get(settings);
        this.httpWorkerCount = Netty4HttpServerTransport.SETTING_HTTP_WORKER_COUNT.get(settings);
    }

    public Settings getSettings() {
        return settings;
    }

    public int getHttpWorkerCount() {
        if (httpWorkerCount == 0) {
            return workerCount;
        } else {
            return httpWorkerCount;
        }
    }

    public int getTransportWorkerCount() {
        return workerCount;
    }

    public synchronized SharedGroup getTransportGroup() {
        return getGenericGroup();
    }

    public synchronized SharedGroup getHttpGroup() {
        if (httpWorkerCount == 0) {
            return getGenericGroup();
        } else {
            NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(httpWorkerCount,
                daemonThreadFactory(settings, HttpServerTransport.HTTP_SERVER_WORKER_THREAD_NAME_PREFIX));

            return new SharedGroup(new RefCountedGroup(eventLoopGroup));
        }
    }

    private SharedGroup getGenericGroup() {
        if (refCountedGroup == null) {
            EventLoopGroup eventLoopGroup = new NioEventLoopGroup(workerCount,
                daemonThreadFactory(settings, TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX));
            this.refCountedGroup = new RefCountedGroup(eventLoopGroup);
            return new SharedGroup(refCountedGroup);
        } else {
            refCountedGroup.incRef();
            return new SharedGroup(refCountedGroup);
        }
    }

    private static class RefCountedGroup extends AbstractRefCounted {

        public static final String NAME = "ref-counted-event-loop-group";
        private final EventLoopGroup eventLoopGroup;

        private RefCountedGroup(EventLoopGroup eventLoopGroup) {
            super(NAME);
            this.eventLoopGroup = eventLoopGroup;
        }

        @Override
        protected void closeInternal() {
            eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
    }

    /**
     * Wraps the {@link RefCountedGroup}. Calls {@link RefCountedGroup#decRef()} on close. After close,
     * this wrapped instance can no longer be used.
     */
    public static class SharedGroup {

        private final RefCountedGroup refCountedGroup;

        private final AtomicBoolean isOpen = new AtomicBoolean(true);

        private SharedGroup(RefCountedGroup refCountedGroup) {
            this.refCountedGroup = refCountedGroup;
        }

        public EventLoopGroup getLowLevelGroup() {
            return refCountedGroup.eventLoopGroup;
        }

        public Future<?> shutdownGracefully() {
            if (isOpen.compareAndSet(true, false)) {
                refCountedGroup.decRef();
                if (refCountedGroup.refCount() == 0) {
                    refCountedGroup.eventLoopGroup.terminationFuture();
                    return refCountedGroup.eventLoopGroup.terminationFuture();
                } else {
                    return getSuccessPromise();
                }
            } else {
                return getSuccessPromise();
            }
        }

        private static Future<?> getSuccessPromise() {
            DefaultPromise<?> promise = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
            promise.setSuccess(null);
            return promise;
        }
    }
}
