/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

/**
 * Cluster state update task configuration for timeout and priority
 */
public interface ClusterStateTaskConfig {
    /**
     * The timeout for this cluster state update task configuration. If the cluster state update task isn't processed within this timeout,
     * the associated {@link ClusterStateTaskListener#onFailure(Exception)} is invoked. Tasks arising from client requests should
     * have a timeout which clients can adjust via the {@code ?master_timeout} query parameter, and typically defaults to {@code 30s}. In
     * contrast, internal tasks can reasonably have an infinite timeout, especially if a timeout would simply trigger a retry.
     *
     * @return the timeout, or null if one is not set
     */
    @Nullable
    TimeValue timeout();

    /**
     * The {@link Priority} for this cluster state update task configuration. Avoid priorites other than {@link Priority#NORMAL} where
     * possible. A stream of higher-priority tasks can starve lower-priority ones from running. Higher-priority tasks should definitely
     * share a {@link ClusterStateTaskExecutor} instance so that they are executed in batches.
     *
     * @return the priority
     */
    Priority priority();

    /**
     * Build a cluster state update task configuration with the specified {@link Priority} and no timeout.
     *
     * @param priority the priority for the associated cluster state update task
     * @return the resulting cluster state update task configuration
     */
    static ClusterStateTaskConfig build(Priority priority) {
        return new Basic(priority, null);
    }

    /**
     * Build a cluster state update task configuration with the specified {@link Priority} and timeout.
     *
     * @param priority the priority for the associated cluster state update task
     * @param timeout  the timeout for the associated cluster state update task
     * @return the result cluster state update task configuration
     */
    static ClusterStateTaskConfig build(Priority priority, TimeValue timeout) {
        return new Basic(priority, timeout);
    }

    record Basic(Priority priority, TimeValue timeout) implements ClusterStateTaskConfig {}
}
