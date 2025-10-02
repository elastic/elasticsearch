/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.threadpool;

import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.common.SettingsBridge;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/**
 * An external bridge for {@link ThreadPool}
 */
public interface ThreadPoolBridge extends StableBridgeAPI<ThreadPool> {

    long relativeTimeInMillis();

    long absoluteTimeInMillis();

    boolean terminate(long timeout, TimeUnit timeUnit);

    static ThreadPoolBridge create(final SettingsBridge bridgedSettings) {
        final ThreadPool internal = new ThreadPool(bridgedSettings.toInternal(), MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        return new ProxyInternal(internal);
    }

    /**
     * An implementation of {@link ThreadPoolBridge} that proxies calls through
     * to an internal {@link ThreadPool}.
     * @see StableBridgeAPI.ProxyInternal
     */
    class ProxyInternal extends StableBridgeAPI.ProxyInternal<ThreadPool> implements ThreadPoolBridge {

        ProxyInternal(final ThreadPool delegate) {
            super(delegate);
        }

        @Override
        public long relativeTimeInMillis() {
            return internalDelegate.relativeTimeInMillis();
        }

        @Override
        public long absoluteTimeInMillis() {
            return internalDelegate.absoluteTimeInMillis();
        }

        @Override
        public boolean terminate(final long timeout, final TimeUnit timeUnit) {
            return ThreadPool.terminate(internalDelegate, timeout, timeUnit);
        }
    }
}
