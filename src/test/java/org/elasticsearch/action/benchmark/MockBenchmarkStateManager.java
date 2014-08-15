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

package org.elasticsearch.action.benchmark;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Mock state manager
 */
public class MockBenchmarkStateManager extends BenchmarkStateManager {

    private final ClusterService clusterService;
    boolean forceFailureOnUpdate = false;

    @Inject
    public MockBenchmarkStateManager(ClusterService clusterService, ThreadPool threadPool, TransportService transportService) {
        super(clusterService, threadPool, transportService);
        this.clusterService = clusterService;
    }

    @Override
    public void update(String benchmarkId, BenchmarkMetaData.State benchmarkState, BenchmarkMetaData.Entry.NodeState nodeState,
                       final ActionListener listener) {

        if (!forceFailureOnUpdate) {
            super.update(benchmarkId, benchmarkState, nodeState, listener);
            return;
        }

        final String cause = "benchmark-update-state-forced-failure (" + benchmarkId + ":" + benchmarkState + ")";

        clusterService.submitStateUpdateTask(cause, new ProcessedClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) { }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                throw new ElasticsearchException("Forced failure");
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }
        });
    }
}
