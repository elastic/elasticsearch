/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty5;

import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.nio.NioHandler;
import io.netty.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty5.Netty5HttpServerTransport;
import org.elasticsearch.transport.TcpTransport;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * Creates and returns {@link io.netty.channel.EventLoopGroup} instances. It will return a shared group for
 * both {@link #getHttpGroup()} and {@link #getTransportGroup()} if
 * {@link Netty5HttpServerTransport#SETTING_HTTP_WORKER_COUNT} is configured to be 0.
 * If that setting is not 0, then it will return a different group in the {@link #getHttpGroup()} call.
 */
public final class SharedGroupFactory {

    private static final Logger logger = LogManager.getLogger(SharedGroupFactory.class);

    private final Settings settings;
    private final int workerCount;
    private final int httpWorkerCount;

    private RefCountedGroup genericGroup;
    private SharedGroup dedicatedHttpGroup;

    public SharedGroupFactory(Settings settings) {
        this.settings = settings;
        this.workerCount = Netty5Transport.WORKER_COUNT.get(settings);
        this.httpWorkerCount = Netty5HttpServerTransport.SETTING_HTTP_WORKER_COUNT.get(settings);
    }

    public Settings getSettings() {
        return settings;
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
            if (dedicatedHttpGroup == null) {
                MultithreadEventLoopGroup eventLoopGroup = new MultithreadEventLoopGroup(
                    httpWorkerCount,
                    daemonThreadFactory(settings, HttpServerTransport.HTTP_SERVER_WORKER_THREAD_NAME_PREFIX),
                    NioHandler.newFactory()
                );
                dedicatedHttpGroup = new SharedGroup(new RefCountedGroup(eventLoopGroup));
            }
            return dedicatedHttpGroup;
        }
    }

    private SharedGroup getGenericGroup() {
        if (genericGroup == null) {
            MultithreadEventLoopGroup eventLoopGroup = new MultithreadEventLoopGroup(
                workerCount,
                EsExecutors.daemonThreadFactory(settings, TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX),
                NioHandler.newFactory()
            );
            this.genericGroup = new RefCountedGroup(eventLoopGroup);
        } else {
            genericGroup.incRef();
        }
        return new SharedGroup(genericGroup);
    }

    private static class RefCountedGroup extends AbstractRefCounted {

        private final MultithreadEventLoopGroup eventLoopGroup;

        private RefCountedGroup(MultithreadEventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = eventLoopGroup;
        }

        @Override
        protected void closeInternal() {
            Future<?> shutdownFuture = eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
            shutdownFuture.awaitUninterruptibly();
            if (shutdownFuture.isSuccess() == false) {
                logger.warn("Error closing netty event loop group", shutdownFuture.cause());
            }
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

        public MultithreadEventLoopGroup getLowLevelGroup() {
            return refCountedGroup.eventLoopGroup;
        }

        public void shutdown() {
            if (isOpen.compareAndSet(true, false)) {
                refCountedGroup.decRef();
            }
        }
    }
}
