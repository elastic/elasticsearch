/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.version;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.version.SystemIndexMappingsVersions;
import org.elasticsearch.action.admin.cluster.version.SystemIndexMappingsVersionsRequest;
import org.elasticsearch.action.admin.cluster.version.SystemIndexMappingsVersionsResponse;
import org.elasticsearch.action.admin.cluster.version.TransportSystemIndexMappingsVersionsAction;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
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
import org.elasticsearch.indices.SystemIndexDescriptor;
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

public class SystemIndexMappingsVersionFixupListener implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SystemIndexMappingsVersionFixupListener.class);

    private static final TimeValue RETRY_TIME = TimeValue.timeValueSeconds(30);

    private final MasterServiceTaskQueue<SystemIndexMappingsVersionTask> taskQueue;
    private final ClusterAdminClient client;
    private final Scheduler scheduler;
    private final Executor executor;
    private final Set<String> pendingNodes = Collections.synchronizedSet(new HashSet<>());

    public SystemIndexMappingsVersionFixupListener(ClusterService service, ClusterAdminClient client, ThreadPool threadPool) {
        // there tends to be a lot of state operations on an upgrade - this one is not time-critical,
        // so use LOW priority. It just needs to be run at some point after upgrade.
        this(
            service.createTaskQueue("fix-system-index-mapping-versions", Priority.LOW, new SystemIndexMappingsVersionUpdater()),
            client,
            threadPool,
            threadPool.executor(ThreadPool.Names.CLUSTER_COORDINATION)
        );
    }

    SystemIndexMappingsVersionFixupListener(
        MasterServiceTaskQueue<SystemIndexMappingsVersionTask> taskQueue,
        ClusterAdminClient client,
        Scheduler scheduler,
        Executor executor
    ) {
        this.taskQueue = taskQueue;
        this.client = client;
        this.scheduler = scheduler;
        this.executor = executor;
    }

    class SystemIndexMappingsVersionTask implements ClusterStateTaskListener {
        private final Map<String, Map<String, SystemIndexDescriptor.MappingsVersion>> results;
        private final int retryNum;

        SystemIndexMappingsVersionTask(Map<String, Map<String, SystemIndexDescriptor.MappingsVersion>> results, int retryNum) {
            this.results = results;
            this.retryNum = retryNum;
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Could not apply system index mappings versions for nodes {} to cluster state", results.keySet(), e);
            scheduleRetry(results.keySet(), retryNum);
        }

        public Map<String, Map<String, SystemIndexDescriptor.MappingsVersion>> results() {
            return results;
        }
    }

    static class SystemIndexMappingsVersionUpdater implements ClusterStateTaskExecutor<SystemIndexMappingsVersionTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<SystemIndexMappingsVersionTask> context) {
            ClusterState.Builder builder = ClusterState.builder(context.initialState());
            var existingCompatibilityVersions = builder.compatibilityVersions();

            boolean modified = false;
            for (var c : context.taskContexts()) {
                for (var e : c.getTask().results().entrySet()) {
                    // double check there are still no SystemIndex mapping versions for the node
                    var nodeCompatibilityVersions = existingCompatibilityVersions.getOrDefault(e.getKey(), CompatibilityVersions.EMPTY);
                    if (nodeCompatibilityVersions.systemIndexMappingsVersion().isEmpty()) {
                        builder.putCompatibilityVersions(
                            e.getKey(),
                            new CompatibilityVersions(nodeCompatibilityVersions.transportVersion(), e.getValue())
                        );
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
             * Check if there are any nodes that should have SystemIndex mapping versions in cluster state, but don't.
             * This can happen if the master was upgraded from before 8.11, and one or more non-master nodes
             * were already upgraded. They don't re-join the cluster with the new master, so never get their CompatibilityVersions
             * updated into cluster state.
             * So we need to do a separate transport call to get the node SystemIndex mapping versions and add them to cluster state.
             * We can't use features to determine when this should happen, as that is unreliable for upgrades (same issue,
             * see NodeFeaturesFixupListener).
             * We also can't use transport version, as that is unreliable for upgrades from versions before 8.8
             * (see TransportVersionFixupListener).
             * So the only thing we can use is release version.
             * This is ok here, as Serverless will never hit this case, so the node feature fetch action will never be called on Serverless.
             * This whole class will be removed in ES v9.
             */
            Set<String> queryNodes = event.state()
                .nodes()
                .stream()
                .filter(n -> n.getVersion().after(Version.V_8_16_0))
                .map(DiscoveryNode::getId)
                .filter(n -> getSystemIndexMappingsVersions(event.state(), n).isEmpty())
                .collect(Collectors.toSet());

            if (queryNodes.isEmpty() == false) {
                logger.debug("Fetching actual system index mappings versions for nodes {}", queryNodes);
                queryNodesSystemIndexMappingsVersions(queryNodes, 0);
            }
        }
    }

    @SuppressForbidden(reason = "Need to access a specific node's system index mappings versions")
    private static Map<String, SystemIndexDescriptor.MappingsVersion> getSystemIndexMappingsVersions(
        ClusterState clusterState,
        String nodeId
    ) {
        var compatibilityVersions = clusterState.compatibilityVersions();
        return compatibilityVersions.getOrDefault(nodeId, CompatibilityVersions.EMPTY).systemIndexMappingsVersion();
    }

    private void scheduleRetry(Set<String> nodes, int thisRetryNum) {
        // just keep retrying until this succeeds
        logger.debug("Scheduling retry {} for nodes {}", thisRetryNum + 1, nodes);
        scheduler.schedule(() -> queryNodesSystemIndexMappingsVersions(nodes, thisRetryNum + 1), RETRY_TIME, executor);
    }

    private void queryNodesSystemIndexMappingsVersions(Set<String> nodes, int retryNum) {
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

        var request = new SystemIndexMappingsVersionsRequest(outstandingNodes.toArray(String[]::new));
        client.execute(TransportSystemIndexMappingsVersionsAction.TYPE, request, new ActionListener<>() {
            @Override
            public void onResponse(SystemIndexMappingsVersionsResponse response) {
                pendingNodes.removeAll(outstandingNodes);
                handleResponse(response, retryNum);
            }

            @Override
            public void onFailure(Exception e) {
                pendingNodes.removeAll(outstandingNodes);
                logger.warn("Could not read system index mappings versions for nodes {}", outstandingNodes, e);
                scheduleRetry(outstandingNodes, retryNum);
            }
        });
    }

    private void handleResponse(SystemIndexMappingsVersionsResponse response, int retryNum) {
        if (response.hasFailures()) {
            Set<String> failedNodes = new HashSet<>();
            for (FailedNodeException fne : response.failures()) {
                logger.warn("Failed to read system index mappings versions from node {}", fne.nodeId(), fne);
                failedNodes.add(fne.nodeId());
            }
            scheduleRetry(failedNodes, retryNum);
        }
        // carry on and read what we can

        Map<String, Map<String, SystemIndexDescriptor.MappingsVersion>> results = response.getNodes()
            .stream()
            .collect(Collectors.toUnmodifiableMap(n -> n.getNode().getId(), SystemIndexMappingsVersions::systemIndexMappingsVersions));

        if (results.isEmpty() == false) {
            taskQueue.submitTask("fix-system-index-mapping-versions", new SystemIndexMappingsVersionTask(results, retryNum), null);
        }
    }
}
