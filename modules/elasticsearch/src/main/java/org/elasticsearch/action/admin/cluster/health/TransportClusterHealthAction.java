/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTableValidation;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * @author kimchy (shay.banon)
 */
public class TransportClusterHealthAction extends TransportMasterNodeOperationAction<ClusterHealthRequest, ClusterHealthResponse> {

    private final ClusterName clusterName;

    @Inject public TransportClusterHealthAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                                ClusterName clusterName) {
        super(settings, transportService, clusterService, threadPool);
        this.clusterName = clusterName;
    }

    @Override protected String executor() {
        return ThreadPool.Names.CACHED;
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Cluster.HEALTH;
    }

    @Override protected ClusterHealthRequest newRequest() {
        return new ClusterHealthRequest();
    }

    @Override protected ClusterHealthResponse newResponse() {
        return new ClusterHealthResponse();
    }

    @Override protected ClusterHealthResponse masterOperation(ClusterHealthRequest request, ClusterState unusedState) throws ElasticSearchException {
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
            return clusterHealth(request, clusterState);
        }
        long endTime = System.currentTimeMillis() + request.timeout().millis();
        while (true) {
            int waitForCounter = 0;
            ClusterState clusterState = clusterService.state();
            ClusterHealthResponse response = clusterHealth(request, clusterState);
            if (request.waitForStatus() != null && response.status().value() <= request.waitForStatus().value()) {
                waitForCounter++;
            }
            if (request.waitForRelocatingShards() != -1 && response.relocatingShards() <= request.waitForRelocatingShards()) {
                waitForCounter++;
            }
            if (request.waitForActiveShards() != -1 && response.activeShards() >= request.waitForActiveShards()) {
                waitForCounter++;
            }
            if (request.indices().length > 0) {
                try {
                    clusterState.metaData().concreteIndices(request.indices());
                    waitForCounter++;
                } catch (IndexMissingException e) {
                    response.status = ClusterHealthStatus.RED; // no indices, make sure its RED
                    // missing indices, wait a bit more...
                }
            }
            if (!request.waitForNodes().isEmpty()) {
                if (request.waitForNodes().startsWith(">=")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(2));
                    if (response.numberOfNodes() >= expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith("ge(")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                    if (response.numberOfNodes() >= expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith("<=")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(2));
                    if (response.numberOfNodes() <= expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith("le(")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                    if (response.numberOfNodes() <= expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith(">")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(1));
                    if (response.numberOfNodes() > expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith("gt(")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                    if (response.numberOfNodes() > expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith("<")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(1));
                    if (response.numberOfNodes() < expected) {
                        waitForCounter++;
                    }
                } else if (request.waitForNodes().startsWith("lt(")) {
                    int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                    if (response.numberOfNodes() < expected) {
                        waitForCounter++;
                    }
                } else {
                    int expected = Integer.parseInt(request.waitForNodes());
                    if (response.numberOfNodes() == expected) {
                        waitForCounter++;
                    }
                }
            }
            if (waitForCounter == waitFor) {
                return response;
            }
            if (System.currentTimeMillis() > endTime) {
                response.timedOut = true;
                return response;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                response.timedOut = true;
                // we got interrupted, bail
                return response;
            }
        }
    }

    private ClusterHealthResponse clusterHealth(ClusterHealthRequest request, ClusterState clusterState) {
        RoutingTableValidation validation = clusterState.routingTable().validate(clusterState.metaData());
        ClusterHealthResponse response = new ClusterHealthResponse(clusterName.value(), validation.failures());
        response.numberOfNodes = clusterState.nodes().size();
        response.numberOfDataNodes = clusterState.nodes().dataNodes().size();

        for (String index : clusterState.metaData().concreteIndicesIgnoreMissing(request.indices())) {
            IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(index);
            IndexMetaData indexMetaData = clusterState.metaData().index(index);
            if (indexRoutingTable == null) {
                continue;
            }
            ClusterIndexHealth indexHealth = new ClusterIndexHealth(index, indexMetaData.numberOfShards(), indexMetaData.numberOfReplicas(), validation.indexFailures(indexMetaData.index()));

            for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                ClusterShardHealth shardHealth = new ClusterShardHealth(shardRoutingTable.shardId().id());
                for (ShardRouting shardRouting : shardRoutingTable) {
                    if (shardRouting.active()) {
                        shardHealth.activeShards++;
                        if (shardRouting.relocating()) {
                            // the shard is relocating, the one he is relocating to will be in initializing state, so we don't count it
                            shardHealth.relocatingShards++;
                        }
                        if (shardRouting.primary()) {
                            shardHealth.primaryActive = true;
                        }
                    } else if (shardRouting.initializing()) {
                        shardHealth.initializingShards++;
                    } else if (shardRouting.unassigned()) {
                        shardHealth.unassignedShards++;
                    }
                }
                if (shardHealth.primaryActive) {
                    if (shardHealth.activeShards == shardRoutingTable.size()) {
                        shardHealth.status = ClusterHealthStatus.GREEN;
                    } else {
                        shardHealth.status = ClusterHealthStatus.YELLOW;
                    }
                } else {
                    shardHealth.status = ClusterHealthStatus.RED;
                }
                indexHealth.shards.put(shardHealth.id(), shardHealth);
            }

            for (ClusterShardHealth shardHealth : indexHealth) {
                if (shardHealth.primaryActive()) {
                    indexHealth.activePrimaryShards++;
                }
                indexHealth.activeShards += shardHealth.activeShards;
                indexHealth.relocatingShards += shardHealth.relocatingShards;
                indexHealth.initializingShards += shardHealth.initializingShards;
                indexHealth.unassignedShards += shardHealth.unassignedShards;
            }
            // update the index status
            indexHealth.status = ClusterHealthStatus.GREEN;
            if (!indexHealth.validationFailures().isEmpty()) {
                indexHealth.status = ClusterHealthStatus.RED;
            } else if (indexHealth.shards().isEmpty()) { // might be since none has been created yet (two phase index creation)
                indexHealth.status = ClusterHealthStatus.RED;
            } else {
                for (ClusterShardHealth shardHealth : indexHealth) {
                    if (shardHealth.status() == ClusterHealthStatus.RED) {
                        indexHealth.status = ClusterHealthStatus.RED;
                        break;
                    }
                    if (shardHealth.status() == ClusterHealthStatus.YELLOW) {
                        indexHealth.status = ClusterHealthStatus.YELLOW;
                    }
                }
            }

            response.indices.put(indexHealth.index(), indexHealth);
        }

        for (ClusterIndexHealth indexHealth : response) {
            response.activePrimaryShards += indexHealth.activePrimaryShards;
            response.activeShards += indexHealth.activeShards;
            response.relocatingShards += indexHealth.relocatingShards;
            response.initializingShards += indexHealth.initializingShards;
            response.unassignedShards += indexHealth.unassignedShards;
        }

        response.status = ClusterHealthStatus.GREEN;
        if (!response.validationFailures().isEmpty()) {
            response.status = ClusterHealthStatus.RED;
        } else if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            response.status = ClusterHealthStatus.RED;
        } else {
            for (ClusterIndexHealth indexHealth : response) {
                if (indexHealth.status() == ClusterHealthStatus.RED) {
                    response.status = ClusterHealthStatus.RED;
                    break;
                }
                if (indexHealth.status() == ClusterHealthStatus.YELLOW) {
                    response.status = ClusterHealthStatus.YELLOW;
                }
            }
        }

        return response;
    }
}
