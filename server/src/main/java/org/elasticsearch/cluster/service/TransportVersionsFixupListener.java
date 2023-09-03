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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.ClusterState.INFERRED_TRANSPORT_VERSION;

/**
 * This fixes up the transport version from pre-8.8.0 cluster state that was inferred as the minimum possible,
 * due to the master node not understanding cluster state with transport versions added in 8.8.0.
 * Any nodes with the inferred placeholder cluster state is then refreshed with their actual transport version
 */
public class TransportVersionsFixupListener implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TransportVersionsFixupListener.class);

    private static final TimeValue RETRY_TIME = TimeValue.timeValueSeconds(30);

    private final MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue;
    private final ClusterAdminClient client;
    private final Scheduler scheduler;
    private final Set<String> pendingNodes = Collections.synchronizedSet(new HashSet<>());

    public TransportVersionsFixupListener(ClusterService service, ClusterAdminClient client, Scheduler scheduler) {
        // there tends to be a lot of state operations on an upgrade - this one is not time-critical,
        // so use LOW priority. It just needs to be run at some point after upgrade.
        this(service.createTaskQueue("fixup-transport-versions", Priority.LOW, new TransportVersionUpdater()), client, scheduler);
    }

    TransportVersionsFixupListener(
        MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue,
        ClusterAdminClient client,
        Scheduler scheduler
    ) {
        this.taskQueue = taskQueue;
        this.client = client;
        this.scheduler = scheduler;
    }

    class NodeTransportVersionTask implements ClusterStateTaskListener {
        private final Map<String, TransportVersion> results;
        private final int retryNum;

        NodeTransportVersionTask(Map<String, TransportVersion> results, int retryNum) {
            this.results = results;
            this.retryNum = retryNum;
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Could not apply transport version for nodes {} to cluster state", results.keySet(), e);
            scheduleRetry(results.keySet(), retryNum);
        }

        public Map<String, TransportVersion> results() {
            return results;
        }
    }

    private static class TransportVersionUpdater implements ClusterStateTaskExecutor<NodeTransportVersionTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<NodeTransportVersionTask> context) throws Exception {
            ClusterState.Builder builder = ClusterState.builder(context.initialState());
            boolean modified = false;
            for (var c : context.taskContexts()) {
                for (var e : c.getTask().results().entrySet()) {
                    // this node's transport version might have been updated already/node has gone away
                    TransportVersion recordedTv = builder.transportVersions().get(e.getKey());
                    assert (recordedTv != null) || (context.initialState().nodes().nodeExists(e.getKey()) == false)
                        : "Node " + e.getKey() + " is in the cluster but does not have an associated transport version recorded";
                    if (Objects.equals(recordedTv, INFERRED_TRANSPORT_VERSION)) {
                        builder.putTransportVersion(e.getKey(), e.getValue());
                        modified = true;
                    }
                }
                c.success(() -> {});
            }
            return modified ? builder.build() : context.initialState();
        }
    }

    @SuppressForbidden(reason = "maintaining ClusterState#transportVersions requires reading them")
    private static Map<String, TransportVersion> getTransportVersions(ClusterState clusterState) {
        return clusterState.transportVersions();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) return; // only if we're master

        // if the min node version > 8.8.0, and the cluster state has some transport versions == 8.8.0,
        // then refresh all inferred transport versions to their real versions
        // now that everything should understand cluster state with transport versions
        if (event.state().nodes().getMinNodeVersion().after(Version.V_8_8_0)
            && event.state().getMinTransportVersion().equals(INFERRED_TRANSPORT_VERSION)) {

            // find all the relevant nodes
            Set<String> nodes = getTransportVersions(event.state()).entrySet()
                .stream()
                .filter(e -> e.getValue().equals(INFERRED_TRANSPORT_VERSION))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            updateTransportVersions(nodes, 0);
        }
    }

    private void scheduleRetry(Set<String> nodes, int thisRetryNum) {
        // just keep retrying until this succeeds
        logger.debug("Scheduling retry {} for nodes {}", thisRetryNum + 1, nodes);
        scheduler.schedule(() -> updateTransportVersions(nodes, thisRetryNum + 1), RETRY_TIME, ThreadPool.Names.CLUSTER_COORDINATION);
    }

    private void updateTransportVersions(Set<String> nodes, int retryNum) {
        // some might already be in-progress
        Set<String> outstandingNodes = Sets.newHashSetWithExpectedSize(nodes.size());
        synchronized (pendingNodes) {
            for (String n : nodes) {
                if (pendingNodes.add(n)) {
                    outstandingNodes.add(n);
                }
            }
        }
        if (outstandingNodes.isEmpty()) {
            // all nodes already have in-progress requests
            return;
        }

        NodesInfoRequest request = new NodesInfoRequest(outstandingNodes.toArray(String[]::new));
        request.clear();    // only requesting base data
        client.nodesInfo(request, new ActionListener<>() {
            @Override
            public void onResponse(NodesInfoResponse response) {
                pendingNodes.removeAll(outstandingNodes);
                handleResponse(response, retryNum);
            }

            @Override
            public void onFailure(Exception e) {
                pendingNodes.removeAll(outstandingNodes);
                logger.warn("Could not read transport versions for nodes {}", outstandingNodes, e);
                scheduleRetry(outstandingNodes, retryNum);
            }
        });
    }

    private void handleResponse(NodesInfoResponse response, int retryNum) {
        if (response.hasFailures()) {
            Set<String> failedNodes = new HashSet<>();
            for (FailedNodeException fne : response.failures()) {
                logger.warn("Failed to read transport version info from node {}", fne.nodeId(), fne);
                failedNodes.add(fne.nodeId());
            }
            scheduleRetry(failedNodes, retryNum);
        }
        // carry on and read what we can

        Map<String, TransportVersion> results = response.getNodes()
            .stream()
            .collect(Collectors.toUnmodifiableMap(n -> n.getNode().getId(), NodeInfo::getTransportVersion));

        if (results.isEmpty() == false) {
            taskQueue.submitTask("update-transport-version", new NodeTransportVersionTask(results, retryNum), null);
        }
    }
}
