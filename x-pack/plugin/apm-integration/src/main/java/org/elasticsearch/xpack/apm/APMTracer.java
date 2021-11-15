/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.plugins.TracingPlugin;
import org.elasticsearch.tasks.Task;

import java.util.Map;

public class APMTracer extends AbstractLifecycleComponent implements TracingPlugin.Tracer {

    private static final Logger logger = LogManager.getLogger();

    private final Map<Long, Releasable> taskSpans = ConcurrentCollections.newConcurrentMap();

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}

    @Override
    public void onTaskRegistered(Task task) {
        taskSpans.computeIfAbsent(task.getId(), taskId -> {
            logger.debug("creating span for task [{}]", taskId);
            return () -> { logger.debug("closing span for task [{}]", taskId); };
        });
    }

    @Override
    public void onTaskUnregistered(Task task) {
        Releasables.close(taskSpans.remove(task.getId()));
    }
}
