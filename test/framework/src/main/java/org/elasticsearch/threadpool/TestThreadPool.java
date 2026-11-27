/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.node.Node;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;

import java.util.concurrent.TimeUnit;

public class TestThreadPool extends ThreadPool implements Releasable {

    public TestThreadPool(String name, ExecutorBuilder<?>... customBuilders) {
        this(name, Settings.EMPTY, customBuilders);
    }

    public TestThreadPool(String name, Settings settings, ExecutorBuilder<?>... customBuilders) {
        this(name, new ThreadContext(settings), settings, customBuilders);
    }

    /** Use this constructor if you need to share the {@link ThreadContext} with some other component, e.g. {@link Tracer}. */
    public TestThreadPool(String name, ThreadContext threadContext, Settings settings, ExecutorBuilder<?>... customBuilders) {
        super(
            threadContext,
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), name).put(settings).build(),
            MeterRegistry.NOOP,
            new DefaultBuiltInExecutorBuilders(),
            customBuilders
        );
    }

    @Override
    public void close() {
        ThreadPool.terminate(this, 10, TimeUnit.SECONDS);
    }
}
