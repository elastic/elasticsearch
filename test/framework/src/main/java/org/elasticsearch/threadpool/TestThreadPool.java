/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.node.Node;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.concurrent.TimeUnit;

public class TestThreadPool extends ThreadPool implements Releasable {

    public TestThreadPool(String name, ExecutorBuilder<?>... customBuilders) {
        this(name, Settings.EMPTY, customBuilders);
    }

    public TestThreadPool(String name, Settings settings, ExecutorBuilder<?>... customBuilders) {
        super(
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
