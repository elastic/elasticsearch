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

public interface ClusterStateTaskConfig {

    /**
     * If the cluster state update task wasn't processed by the provided timeout, call
     * {@link ClusterStateTaskListener#onFailure(String, Throwable)}. May return null to indicate no timeout is needed (default).
     */
    @Nullable
    TimeValue timeout();

    Priority priority();

    static ClusterStateTaskConfig build(Priority priority) {
        return new Basic(priority, null);
    }

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
