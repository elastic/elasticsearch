/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;

/**
 * Cluster state update task configuration for timeout and priority
 */
public interface ClusterStateTaskConfig {
    /**
     * The timeout for this cluster state update task configuration. If
     * the cluster state update task isn't processed within this
     * timeout, the associated {@link ClusterStateTaskListener#onFailure(String, Exception)}
     * is invoked.
     *
     * @return the timeout, or null if one is not set
     */
    @Nullable
    TimeValue timeout();

    /**
     * The {@link Priority} for this cluster state update task configuration.
     *
     * @return the priority
     */
    Priority priority();

    /**
     * Build a cluster state update task configuration with the
     * specified {@link Priority} and no timeout.
     *
     * @param priority the priority for the associated cluster state
     *                 update task
     * @return the resulting cluster state update task configuration
     */
    static ClusterStateTaskConfig build(Priority priority) {
        return new Basic(priority, null);
    }

    /**
     * Build a cluster state update task configuration with the
     * specified {@link Priority} and timeout.
     *
     * @param priority the priority for the associated cluster state
     *                 update task
     * @param timeout  the timeout for the associated cluster state
     *                 update task
     * @return the result cluster state update task configuration
     */
    static ClusterStateTaskConfig build(Priority priority, TimeValue timeout) {
        return new Basic(priority, timeout);
    }

    class Basic implements ClusterStateTaskConfig {
        final TimeValue timeout;
        final Priority priority;

        public Basic(Priority priority, TimeValue timeout) {
            this.timeout = timeout;
            this.priority = priority;
        }

        @Override
        public TimeValue timeout() {
            return timeout;
        }

        @Override
        public Priority priority() {
            return priority;
        }
    }
}
