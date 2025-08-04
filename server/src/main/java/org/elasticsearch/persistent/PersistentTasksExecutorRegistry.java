/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.persistent;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Strings;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Components that registers all persistent task executors
 */
public class PersistentTasksExecutorRegistry {

    private static final Set<String> CLUSTER_SCOPED_TASKS = ConcurrentCollections.newConcurrentSet();

    private final Map<String, PersistentTasksExecutor<?>> taskExecutors;

    public PersistentTasksExecutorRegistry(Collection<PersistentTasksExecutor<?>> taskExecutors) {
        Map<String, PersistentTasksExecutor<?>> map = new HashMap<>();
        for (PersistentTasksExecutor<?> executor : taskExecutors) {
            final var old = map.put(executor.getTaskName(), executor);
            if (old != null) {
                final var message = Strings.format(
                    "task [%s] is already registered with [%s], cannot re-register with [%s]",
                    executor.getTaskName(),
                    old,
                    executor
                );
                assert false : message;
                throw new IllegalStateException(message);
            }
            if (executor.scope() == PersistentTasksExecutor.Scope.CLUSTER) {
                CLUSTER_SCOPED_TASKS.add(executor.getTaskName());
            }
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

    public static boolean isClusterScopedTask(String taskName) {
        return CLUSTER_SCOPED_TASKS.contains(taskName);
    }
}
