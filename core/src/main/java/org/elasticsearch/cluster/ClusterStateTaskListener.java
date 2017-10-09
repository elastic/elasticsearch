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

import org.elasticsearch.cluster.service.MasterService;

import java.util.List;

public interface ClusterStateTaskListener {

    /**
     * A callback called when execute fails.
     */
    void onFailure(String source, Exception e);

    /**
     * called when the task was rejected because the local node is no longer master.
     * Used only for tasks submitted to {@link MasterService}.
     */
    default void onNoLongerMaster(String source) {
        onFailure(source, new NotMasterException("no longer master. source: [" + source + "]"));
    }

    /**
     * Called when the result of the {@link ClusterStateTaskExecutor#execute(ClusterState, List)} have been processed
     * properly by all listeners.
     */
    default void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
    }
}
