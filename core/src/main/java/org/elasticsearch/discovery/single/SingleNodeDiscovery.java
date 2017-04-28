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

package org.elasticsearch.discovery.single;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.zen.PendingClusterStateStats;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

/**
 * A discovery implementation where the only member of the cluster is the local node.
 */
public class SingleNodeDiscovery extends AbstractLifecycleComponent implements Discovery {

    protected final TransportService transportService;
    private final ClusterApplier clusterApplier;
    protected volatile ClusterState initialState;
    private volatile ClusterState clusterState;

    public SingleNodeDiscovery(final Settings settings, final TransportService transportService,
                               ClusterApplier clusterApplier) {
        super(Objects.requireNonNull(settings));
        this.transportService = Objects.requireNonNull(transportService);
        this.clusterApplier = clusterApplier;
    }

    @Override
    public void setAllocationService(final AllocationService allocationService) {

    }

    @Override
    public synchronized void publish(final ClusterChangedEvent event,
                                     final AckListener ackListener) {
        clusterState = event.state();
        CountDownLatch latch = new CountDownLatch(1);

        ClusterStateTaskListener listener = new ClusterStateTaskListener() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
                ackListener.onNodeAck(transportService.getLocalNode(), null);
            }

            @Override
            public void onFailure(String source, Exception e) {
                latch.countDown();
                ackListener.onNodeAck(transportService.getLocalNode(), e);
                logger.warn(
                    (org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage(
                        "failed while applying cluster state locally [{}]",
                        event.source()),
                    e);
            }
        };
        clusterApplier.onNewClusterState("apply-locally-on-node[" + event.source() + "]", this::clusterState, listener);

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public synchronized ClusterState getInitialClusterState() {
        if (initialState == null) {
            DiscoveryNode localNode = transportService.getLocalNode();
            initialState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
                .nodes(DiscoveryNodes.builder().add(localNode)
                    .localNodeId(localNode.getId())
                    .masterNodeId(localNode.getId())
                    .build())
                .blocks(ClusterBlocks.builder()
                    .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK))
                .build();
        }
        return initialState;
    }

    @Override
    public ClusterState clusterState() {
        return clusterState;
    }

    @Override
    public DiscoveryStats stats() {
        return new DiscoveryStats((PendingClusterStateStats) null);
    }

    @Override
    public synchronized void startInitialJoin() {
        // apply a fresh cluster state just so that state recovery gets triggered by GatewayService
        // TODO: give discovery module control over GatewayService
        clusterState = ClusterState.builder(getInitialClusterState()).build();
        clusterApplier.onNewClusterState("single-node-start-initial-join", this::clusterState, (source, e) -> {});
    }

    @Override
    public int getMinimumMasterNodes() {
        return 1;
    }

    @Override
    protected synchronized void doStart() {
        initialState = getInitialClusterState();
        clusterState = initialState;
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

}
