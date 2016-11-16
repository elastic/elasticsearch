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

/**
 * An interface for specifying tasks to be executed by the cluster service.
 */
public interface ClusterServiceTaskExecutor {

    /** A cached cluster task result that indicates the cluster has no master and a block should be added. */
    ClusterServiceTaskResult NO_MASTER_RESULT = new ClusterServiceTaskResult(true);
    /** A cached cluster task result that indicates the cluster does have a master, nothing to change with the blocks. */
    ClusterServiceTaskResult HAS_MASTER_RESULT = new ClusterServiceTaskResult(false);

    /**
     * The method to be executed by the cluster service, operating on the current cluster state passed in.
     */
    ClusterServiceTaskResult execute(ClusterState currentState) throws Exception;

    /**
     * Indicates whether this task should only run if current node is master.  Defaults to {@code false}.
     */
    default boolean runOnlyOnMaster() {
        return false;
    }

    /**
     * A class that represents the result of running a task on the cluster service.
     */
    final class ClusterServiceTaskResult {
        private final boolean hasNoMaster;

        private ClusterServiceTaskResult(boolean hasNoMaster) {
            this.hasNoMaster = hasNoMaster;
        }

        /**
         * Returns {@code true} if the cluster service task should set the no master block on the local state,
         * {@code false} otherwise.
         */
        public boolean hasNoMaster() {
            return hasNoMaster;
        }
    }
}
