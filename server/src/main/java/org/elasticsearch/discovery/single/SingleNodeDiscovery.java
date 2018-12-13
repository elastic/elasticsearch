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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplier.ClusterApplyListener;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.transport.TransportService;

import java.util.Objects;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

/**
 * A discovery implementation where the only member of the cluster is the local node.
 */
public class SingleNodeDiscovery extends AbstractLifecycleComponent implements Discovery {
    private static final Logger logger = LogManager.getLogger(SingleNodeDiscovery.class);

    private final ClusterName clusterName;
    protected final TransportService transportService;
    private final ClusterApplier clusterApplier;
    private volatile ClusterState clusterState;

    public SingleNodeDiscovery(final Settings settings, final TransportService transportService,
                               final MasterService masterService, final ClusterApplier clusterApplier) {
        super(Objects.requireNonNull(settings));
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.transportService = Objects.requireNonNull(transportService);
        masterService.setClusterStateSupplier(() -> clusterState);
        this.clusterApplier = clusterApplier;
    }

    @Override
    public synchronized void publish(final ClusterChangedEvent event, ActionListener<Void> publishListener,
                                     final AckListener ackListener) {
        clusterState = event.state();
        ackListener.onCommit(TimeValue.ZERO);

        clusterApplier.onNewClusterState("apply-locally-on-node[" + event.source() + "]", () -> clusterState, new ClusterApplyListener() {
            @Override
            public void onSuccess(String source) {
                publishListener.onResponse(null);
                ackListener.onNodeAck(transportService.getLocalNode(), null);
            }

            @Override
            public void onFailure(String source, Exception e) {
                publishListener.onFailure(e);
                ackListener.onNodeAck(transportService.getLocalNode(), e);
                logger.warn(() -> new ParameterizedMessage("failed while applying cluster state locally [{}]", event.source()), e);
            }
        });
    }

    @Override
    public DiscoveryStats stats() {
        return new DiscoveryStats(null, null);
    }

    @Override
    public synchronized void startInitialJoin() {
        if (lifecycle.started() == false) {
            throw new IllegalStateException("can't start initial join when not started");
        }
        // apply a fresh cluster state just so that state recovery gets triggered by GatewayService
        // TODO: give discovery module control over GatewayService
        clusterState = ClusterState.builder(clusterState).build();
        clusterApplier.onNewClusterState("single-node-start-initial-join", () -> clusterState, (source, e) -> {});
    }

    @Override
    protected synchronized void doStart() {
        // set initial state
        DiscoveryNode localNode = transportService.getLocalNode();
        clusterState = createInitialState(localNode);
        clusterApplier.setInitialState(clusterState);
    }

    protected ClusterState createInitialState(DiscoveryNode localNode) {
        ClusterState.Builder builder = ClusterState.builder(clusterName);
        return builder.nodes(DiscoveryNodes.builder().add(localNode)
                .localNodeId(localNode.getId())
                .masterNodeId(localNode.getId())
                .build())
            .blocks(ClusterBlocks.builder()
                .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK))
            .build();
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {

    }

}
