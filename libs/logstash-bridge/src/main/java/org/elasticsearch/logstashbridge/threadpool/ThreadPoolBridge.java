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

public class ThreadPoolBridge extends StableBridgeAPI.Proxy<ThreadPool> {

    public ThreadPoolBridge(final SettingsBridge settingsBridge) {
        this(new ThreadPool(settingsBridge.unwrap(), MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders()));
    }

    public ThreadPoolBridge(final ThreadPool delegate) {
        super(delegate);
    }

    public static boolean terminate(final ThreadPoolBridge pool, final long timeout, final TimeUnit timeUnit) {
        return ThreadPool.terminate(pool.unwrap(), timeout, timeUnit);
    }

    public long relativeTimeInMillis() {
        return delegate.relativeTimeInMillis();
    }

    public long absoluteTimeInMillis() {
        return delegate.absoluteTimeInMillis();
    }
}
