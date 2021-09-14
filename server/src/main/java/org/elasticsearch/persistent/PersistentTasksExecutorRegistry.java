/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Components that registers all persistent task executors
 */
public class PersistentTasksExecutorRegistry {

    private final Map<String, PersistentTasksExecutor<?>> taskExecutors;

    public PersistentTasksExecutorRegistry(Collection<PersistentTasksExecutor<?>> taskExecutors) {
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
