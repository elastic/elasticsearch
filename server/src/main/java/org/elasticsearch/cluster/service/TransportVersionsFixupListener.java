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
import org.elasticsearch.action.FailedNodeException;
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
    private final Set<String> pendingNodes = Collections.synchronizedSet(new HashSet<>());

    public TransportVersionsFixupListener(ClusterService service, ClusterAdminClient client) {
        masterService = service.createTaskQueue("fixup-transport-versions", Priority.LOW, new TransportVersionUpdater());
        this.client = client;
    }

    private record NodeTransportVersionTask(Map<String, TransportVersion> results) implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            Log.error("Could not apply transport version for nodes {} to cluster state", results.keySet(), e);
        }
    }

    private static class TransportVersionUpdater implements ClusterStateTaskExecutor<NodeTransportVersionTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<NodeTransportVersionTask> context) throws Exception {
            ClusterState.Builder builder = ClusterState.builder(context.initialState());
            for (var c : context.taskContexts()) {
                for (var e : c.getTask().results().entrySet()) {
                    // this node's transport version might have been updated already/node has gone away
                    if (Objects.equals(builder.transportVersions().get(e.getKey()), INFERRED_VERSION)) {
                        builder.putTransportVersion(e.getKey(), e.getValue());
                    }
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
            Set<String> nodes = new HashSet<>();
            synchronized (pendingNodes) {
                for (Map.Entry<String, TransportVersion> e : event.state().transportVersions().entrySet()) {
                    if (e.getValue().equals(INFERRED_VERSION) && pendingNodes.add(e.getKey())) {
                        nodes.add(e.getKey());
                    }
                }
            }
            if (nodes.isEmpty()) {
                // all nodes already got in-progress requests
                return;
            }

            NodesInfoRequest request = new NodesInfoRequest(nodes.toArray(String[]::new));
            request.clear().addMetric(ClusterState.Metric.NODES.toString());
            client.nodesInfo(request, new ActionListener<>() {
                @Override
                public void onResponse(NodesInfoResponse response) {
                    pendingNodes.removeAll(nodes);
                    handleResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    pendingNodes.removeAll(nodes);
                    Log.warn("Could not read transport versions from nodes", e);
                }
            });
        }
    }

    private void handleResponse(NodesInfoResponse response) {
        for (FailedNodeException fne : response.failures()) {
            Log.warn("Failed to read transport version info from node {}", fne.nodeId(), fne);
        }
        // carry on and read what we can

        Map<String, TransportVersion> results = response.getNodes()
            .stream()
            .collect(Collectors.toUnmodifiableMap(n -> n.getNode().getId(), NodeInfo::getTransportVersion));

        if (results.isEmpty() == false) {
            masterService.submitTask("update-transport-version", new NodeTransportVersionTask(results), null);
        }
    }
}
