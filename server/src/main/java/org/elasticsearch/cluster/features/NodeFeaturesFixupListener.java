/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.features;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.features.NodeFeatures;
import org.elasticsearch.action.admin.cluster.node.features.NodesFeaturesRequest;
import org.elasticsearch.action.admin.cluster.node.features.NodesFeaturesResponse;
import org.elasticsearch.action.admin.cluster.node.features.TransportNodesFeaturesAction;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterFeatures;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

@UpdateForV9    // this can be removed in v9
public class NodeFeaturesFixupListener implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(NodeFeaturesFixupListener.class);

    private static final TimeValue RETRY_TIME = TimeValue.timeValueSeconds(30);

    private final MasterServiceTaskQueue<NodesFeaturesTask> taskQueue;
    private final ClusterAdminClient client;
    private final Scheduler scheduler;
    private final Executor executor;
    private final Set<String> pendingNodes = Collections.synchronizedSet(new HashSet<>());

    public NodeFeaturesFixupListener(ClusterService service, ClusterAdminClient client, ThreadPool threadPool) {
        // there tends to be a lot of state operations on an upgrade - this one is not time-critical,
        // so use LOW priority. It just needs to be run at some point after upgrade.
        this(
            service.createTaskQueue("fix-node-features", Priority.LOW, new NodesFeaturesUpdater()),
            client,
            threadPool,
            threadPool.executor(ThreadPool.Names.CLUSTER_COORDINATION)
        );
    }

    NodeFeaturesFixupListener(
        MasterServiceTaskQueue<NodesFeaturesTask> taskQueue,
        ClusterAdminClient client,
        Scheduler scheduler,
        Executor executor
    ) {
        this.taskQueue = taskQueue;
        this.client = client;
        this.scheduler = scheduler;
        this.executor = executor;
    }

    class NodesFeaturesTask implements ClusterStateTaskListener {
        private final Map<String, Set<String>> results;
        private final int retryNum;

        NodesFeaturesTask(Map<String, Set<String>> results, int retryNum) {
            this.results = results;
            this.retryNum = retryNum;
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Could not apply features for nodes {} to cluster state", results.keySet(), e);
            scheduleRetry(results.keySet(), retryNum);
        }

        public Map<String, Set<String>> results() {
            return results;
        }
    }

    static class NodesFeaturesUpdater implements ClusterStateTaskExecutor<NodesFeaturesTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<NodesFeaturesTask> context) {
            ClusterState.Builder builder = ClusterState.builder(context.initialState());
            var existingFeatures = builder.nodeFeatures();

            boolean modified = false;
            for (var c : context.taskContexts()) {
                for (var e : c.getTask().results().entrySet()) {
                    // double check there are still no features for the node
                    if (existingFeatures.getOrDefault(e.getKey(), Set.of()).isEmpty()) {
                        builder.putNodeFeatures(e.getKey(), e.getValue());
                        modified = true;
                    }
                }
                c.success(() -> {});
            }
            return modified ? builder.build() : context.initialState();
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesDelta().masterNodeChanged() && event.localNodeMaster()) {
            /*
             * Execute this if we have just become master.
             * Check if there are any nodes that should have features in cluster state, but don't.
             * This can happen if the master was upgraded from before 8.13, and one or more non-master nodes
             * were already upgraded. They don't re-join the cluster with the new master, so never get their features
             * (which the master now understands) added to cluster state.
             * So we need to do a separate transport call to get the node features and add them to cluster state.
             * We can't use features to determine when this should happen, as the features are incorrect.
             * We also can't use transport version, as that is unreliable for upgrades
             * from versions before 8.8 (see TransportVersionFixupListener).
             * So the only thing we can use is release version.
             * This is ok here, as Serverless will never hit this case, so the node feature fetch action will never be called on Serverless.
             * This whole class will be removed in ES v9.
             */
            ClusterFeatures nodeFeatures = event.state().clusterFeatures();
            Set<String> queryNodes = event.state()
                .nodes()
                .stream()
                .filter(n -> n.getVersion().onOrAfter(Version.V_8_15_0))
                .map(DiscoveryNode::getId)
                .filter(n -> getNodeFeatures(nodeFeatures, n).isEmpty())
                .collect(Collectors.toSet());

            if (queryNodes.isEmpty() == false) {
                logger.debug("Fetching actual node features for nodes {}", queryNodes);
                queryNodesFeatures(queryNodes, 0);
            }
        }
    }

    @SuppressForbidden(reason = "Need to access a specific node's features")
    private static Set<String> getNodeFeatures(ClusterFeatures features, String nodeId) {
        return features.nodeFeatures().getOrDefault(nodeId, Set.of());
    }

    private void scheduleRetry(Set<String> nodes, int thisRetryNum) {
        // just keep retrying until this succeeds
        logger.debug("Scheduling retry {} for nodes {}", thisRetryNum + 1, nodes);
        scheduler.schedule(() -> queryNodesFeatures(nodes, thisRetryNum + 1), RETRY_TIME, executor);
    }

    private void queryNodesFeatures(Set<String> nodes, int retryNum) {
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

        NodesFeaturesRequest request = new NodesFeaturesRequest(outstandingNodes.toArray(String[]::new));
        client.execute(TransportNodesFeaturesAction.TYPE, request, new ActionListener<>() {
            @Override
            public void onResponse(NodesFeaturesResponse response) {
                pendingNodes.removeAll(outstandingNodes);
                handleResponse(response, retryNum);
            }

            @Override
            public void onFailure(Exception e) {
                pendingNodes.removeAll(outstandingNodes);
                logger.warn("Could not read features for nodes {}", outstandingNodes, e);
                scheduleRetry(outstandingNodes, retryNum);
            }
        });
    }

    private void handleResponse(NodesFeaturesResponse response, int retryNum) {
        if (response.hasFailures()) {
            Set<String> failedNodes = new HashSet<>();
            for (FailedNodeException fne : response.failures()) {
                logger.warn("Failed to read features from node {}", fne.nodeId(), fne);
                failedNodes.add(fne.nodeId());
            }
            scheduleRetry(failedNodes, retryNum);
        }
        // carry on and read what we can

        Map<String, Set<String>> results = response.getNodes()
            .stream()
            .collect(Collectors.toUnmodifiableMap(n -> n.getNode().getId(), NodeFeatures::nodeFeatures));

        if (results.isEmpty() == false) {
            taskQueue.submitTask("fix-node-features", new NodesFeaturesTask(results, retryNum), null);
        }
    }
}
