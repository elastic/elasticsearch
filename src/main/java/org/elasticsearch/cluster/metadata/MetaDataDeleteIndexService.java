/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.metadata.MetaData.newMetaDataBuilder;

/**
 * Service responsible for submitting delete index requests
 */
public class MetaDataDeleteIndexService extends AbstractComponent {

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final MetaDataService metaDataService;

    @Inject
    public MetaDataDeleteIndexService(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                      AllocationService allocationService, MetaDataService metaDataService) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.metaDataService = metaDataService;
    }

    public void deleteIndex(final DeleteIndexClusterStateUpdateRequest request, final ClusterStateUpdateListener<ClusterStateUpdateResponse> listener) {
        // we lock here, and not within the cluster service callback since we don't want to
        // block the whole cluster state handling
        final Semaphore mdLock = metaDataService.indexMetaDataLock(request.index());

        // quick check to see if we can acquire a lock, otherwise spawn to a thread pool
        if (mdLock.tryAcquire()) {
            deleteIndex(request, listener, mdLock);
            return;
        }

        threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!mdLock.tryAcquire(request.masterNodeTimeout().nanos(), TimeUnit.NANOSECONDS)) {
                        listener.onFailure(new ProcessClusterEventTimeoutException(request.masterNodeTimeout(), "acquire index lock"));
                        return;
                    }
                } catch (InterruptedException e) {
                    listener.onFailure(e);
                    return;
                }

                deleteIndex(request, listener, mdLock);
            }
        });
    }

    private void deleteIndex(final DeleteIndexClusterStateUpdateRequest request, final ClusterStateUpdateListener<ClusterStateUpdateResponse> listener, final Semaphore mdLock) {
        clusterService.submitStateUpdateTask("delete-index [" + request.index() + "]", Priority.URGENT, new AckedClusterStateUpdateTask() {

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return true;
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                mdLock.release();
                listener.onResponse(new ClusterStateUpdateResponse(true));
            }

            @Override
            public void onAckTimeout() {
                mdLock.release();
                listener.onResponse(new ClusterStateUpdateResponse(false));
            }

            @Override
            public TimeValue ackTimeout() {
                return request.ackTimeout();
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                mdLock.release();
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {
                if (!currentState.metaData().hasConcreteIndex(request.index())) {
                    throw new IndexMissingException(new Index(request.index()));
                }

                logger.info("[{}] deleting index", request.index());

                RoutingTable.Builder routingTableBuilder = RoutingTable.builder().routingTable(currentState.routingTable());
                routingTableBuilder.remove(request.index());

                MetaData newMetaData = newMetaDataBuilder()
                        .metaData(currentState.metaData())
                        .remove(request.index())
                        .build();

                RoutingAllocation.Result routingResult = allocationService.reroute(
                        newClusterStateBuilder().state(currentState).routingTable(routingTableBuilder).metaData(newMetaData).build());

                ClusterBlocks blocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeIndexBlocks(request.index()).build();
                return newClusterStateBuilder().state(currentState).routingResult(routingResult).metaData(newMetaData).blocks(blocks).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            }
        });
    }
}
