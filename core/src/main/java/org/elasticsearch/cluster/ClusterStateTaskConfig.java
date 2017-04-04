/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;

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
