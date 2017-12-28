/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Components that registers all persistent task executors
 */
public class PersistentTasksExecutorRegistry extends AbstractComponent {

    private final Map<String, PersistentTasksExecutor<?>> taskExecutors;

    @SuppressWarnings("unchecked")
    public PersistentTasksExecutorRegistry(Settings settings, Collection<PersistentTasksExecutor<?>> taskExecutors) {
        super(settings);
        Map<String, PersistentTasksExecutor<?>> map = new HashMap<>();
        for (PersistentTasksExecutor<?> executor : taskExecutors) {
            map.put(executor.getTaskName(), executor);
        }
        this.taskExecutors = Collections.unmodifiableMap(map);
    }

    @SuppressWarnings("unchecked")
    public <Params extends PersistentTaskParams> PersistentTasksExecutor<Params> getPersistentTaskExecutorSafe(String taskName) {
        PersistentTasksExecutor<Params> executor = (PersistentTasksExecutor<Params>) taskExecutors.get(taskName);
        if (executor == null) {
            throw new IllegalStateException("Unknown persistent executor [" + taskName + "]");
        }
        return executor;
    }
}