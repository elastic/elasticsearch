/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.common.Priority;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Map;
import java.util.Objects;

/**
 * This fixes up the transport version from pre-8.8.0 cluster state that was inferred as the minimum possible,
 * due to the master node not understanding cluster state with transport versions added in 8.8.0.
 * Any nodes with the inferred placeholder cluster state is then refreshed with their actual transport version
 */
public class TransportVersionsFixupListener implements ClusterStateListener {

    private static final Logger Log = LogManager.getLogger(TransportVersionsFixupListener.class);

    // TODO fill in the version indicating an inferred version
    private static final TransportVersion INFERRED_VERSION = TransportVersion.V_8_8_0;

    private final MasterServiceTaskQueue<NodeTransportVersionTask> masterService;
    private final ClusterAdminClient client;

    public TransportVersionsFixupListener(ClusterService service, ClusterAdminClient client) {
        masterService = service.createTaskQueue("fixup-transport-versions", Priority.LANGUID, new TransportVersionUpdater());
        this.client = client;
    }

    private record NodeTransportVersionTask(String nodeId, TransportVersion version) implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            Log.error("Could not apply transport version for node {} to cluster state", nodeId, e);
        }
    }

    private static class TransportVersionUpdater implements ClusterStateTaskExecutor<NodeTransportVersionTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<NodeTransportVersionTask> context) throws Exception {
            ClusterState.Builder builder = ClusterState.builder(context.initialState());
            for (var c : context.taskContexts()) {
                var t = c.getTask();
                // this node's transport version might have been updated already/node has gone away
                if (Objects.equals(builder.transportVersions().get(t.nodeId()), INFERRED_VERSION)) {
                    builder.putTransportVersion(t.nodeId(), t.version());
                }
            }
            return builder.build();
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) return; // only if we're master

        // if the min node version >= 8.8.0, and the min transport version is inferred,
        // then refresh all inferred transport versions to their real versions
        // now that everything should understand cluster state with transport versions
        if (event.state().nodes().getMinNodeVersion().onOrAfter(Version.V_8_8_0)
            && event.state().getMinTransportVersion().equals(INFERRED_VERSION)) {
            // find all the relevant nodes
            String[] nodes = event.state()
                .transportVersions()
                .entrySet()
                .stream()
                .filter(e -> e.getValue().equals(INFERRED_VERSION))
                .map(Map.Entry::getKey)
                .toArray(String[]::new);

            NodesInfoRequest request = new NodesInfoRequest(nodes);
            request.clear().addMetric(ClusterState.Metric.NODES.toString());
            client.nodesInfo(request, new ActionListener<>() {
                @Override
                public void onResponse(NodesInfoResponse response) {
                    for (NodeInfo n : response.getNodes()) {
                        String nodeId = n.getNode().getId();
                        masterService.submitTask(
                            "update-transport-version[" + nodeId + "]",
                            new NodeTransportVersionTask(nodeId, n.getTransportVersion()),
                            null
                        );
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    Log.warn("Could not read transport versions from nodes", e);
                }
            });
        }
    }
}
