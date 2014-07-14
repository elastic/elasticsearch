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

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TransportClusterHealthAction extends TransportMasterNodeReadOperationAction<ClusterHealthRequest, ClusterHealthResponse> {

    private final ClusterName clusterName;

    @Inject
    public TransportClusterHealthAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                        ClusterName clusterName) {
        super(settings, ClusterHealthAction.NAME, transportService, clusterService, threadPool);
        this.clusterName = clusterName;
    }

    @Override
    protected String executor() {
        // we block here...
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected ClusterHealthRequest newRequest() {
        return new ClusterHealthRequest();
    }

    @Override
    protected ClusterHealthResponse newResponse() {
        return new ClusterHealthResponse();
    }

    @Override
    protected void masterOperation(final ClusterHealthRequest request, final ClusterState unusedState, final ActionListener<ClusterHealthResponse> listener) throws ElasticsearchException {
        long endTime = System.currentTimeMillis() + request.timeout().millis();

        if (request.waitForEvents() != null) {
            final CountDownLatch latch = new CountDownLatch(1);
            clusterService.submitStateUpdateTask("cluster_health (wait_for_events [" + request.waitForEvents() + "])", request.waitForEvents(), new ProcessedClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
                }
            });

            try {
                latch.await(request.timeout().millis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        }


        int waitFor = 5;
        if (request.waitForStatus() == null) {
            waitFor--;
        }
        if (request.waitForRelocatingShards() == -1) {
            waitFor--;
        }
        if (request.waitForActiveShards() == -1) {
            waitFor--;
        }
        if (request.waitForNodes().isEmpty()) {
            waitFor--;
        }
        if (request.indices().length == 0) { // check that they actually exists in the meta data
            waitFor--;
        }
        if (waitFor == 0) {
            // no need to wait for anything
            ClusterState clusterState = clusterService.state();
            listener.onResponse(clusterHealth(request, clusterState));
            return;
        }
        while (true) {
            int waitForCounter = 0;
            ClusterState clusterState = clusterService.state();
            ClusterHealthResponse response = clusterHealth(request, clusterState);
            if (request.waitForStatus() != null && response.getStatus().value() <= request.waitForStatus().value()) {
                waitForCounter++;
            }
            if (request.waitForRelocatingShards() != -1 && response.getRelocatingShards() <= request.waitForRelocatingShards()) {
                waitForCounter++;
            }
            if (request.waitForActiveShards() != -1 && response.getActiveShards() >= request.waitForActiveShards()) {
                waitForCounter++;
            }
            if (request.indices().length > 0) {
                try {
                    clusterState.metaData().concreteIndices(IndicesOptions.strictExpand(), request.indices());
                    waitForCounter++;
                } catch (IndexMissingException e) {
                    response.status = ClusterHealthStatus.RED; // no indices, make sure its RED
                    // missing indices, wait a bit more...
                }
            }
            if (!request.waitForNodes().isEmpty()) {
                if (request.waitForNodes().startsWith(">=")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(2));
                    if (response.getNumberOfNodes() >= expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith("ge(")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                    if (response.getNumberOfNodes() >= expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith("<=")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(2));
                    if (response.getNumberOfNodes() <= expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith("le(")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                    if (response.getNumberOfNodes() <= expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith(">")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(1));
                    if (response.getNumberOfNodes() > expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith("gt(")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                    if (response.getNumberOfNodes() > expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith("<")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(1));
                    if (response.getNumberOfNodes() < expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith("lt(")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                    if (response.getNumberOfNodes() < expected) {
                        waitForCounter++;
                    }
                } else {
                    int expected = Integer.parseInt(request.waitForNodes());
                    if (response.getNumberOfNodes() == expected) {
                        waitForCounter++;
                    }
                }
            }
            if (waitForCounter == waitFor) {
                listener.onResponse(response);
                return;
            }
            if (System.currentTimeMillis() > endTime) {
                response.timedOut = true;
                listener.onResponse(response);
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                response.timedOut = true;
                listener.onResponse(response);
                return;
            }
        }
    }


    private ClusterHealthResponse clusterHealth(ClusterHealthRequest request, ClusterState clusterState) {
        if (logger.isTraceEnabled()) {
            logger.trace("Calculating health based on state version [{}]", clusterState.version());
        }
        String[] concreteIndices;
        try {
            concreteIndices = clusterState.metaData().concreteIndices(IndicesOptions.lenientExpandOpen(), request.indices());
        } catch (IndexMissingException e) {
            // one of the specified indices is not there - treat it as RED.
            ClusterHealthResponse response = new ClusterHealthResponse(clusterName.value(), Strings.EMPTY_ARRAY, clusterState);
            response.status = ClusterHealthStatus.RED;
            return response;
        }

        return new ClusterHealthResponse(clusterName.value(), concreteIndices, clusterState);
    }
}
