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

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;

public class BasicClusterStateTaskConfig implements ClusterStateTaskConfig {
    private final TimeValue timeout;

    @Override
    public TimeValue timeout() {
        return timeout;
    }

    private final Priority priority;

    @Override
    public Priority priority() {
        return priority;
    }

    private BasicClusterStateTaskConfig(TimeValue timeout, Priority priority) {
        this.timeout = timeout;
        this.priority = priority;
    }

    /**
     * Build a cluster state update task configuration with the
     * specified {@link Priority} and no timeout.
     *
     * @param priority the priority for the associated cluster state
     *                 update task
     * @return the resulting cluster state update task configuration
     */
    public static ClusterStateTaskConfig create(Priority priority) {
        return create(priority, null);
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
    public static ClusterStateTaskConfig create(Priority priority, TimeValue timeout) {
        return new BasicClusterStateTaskConfig(timeout, priority);
    }
}
