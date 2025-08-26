/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.ClusterState.INFERRED_TRANSPORT_VERSION;

/**
 * This fixes up the compatibility versions (transport version and system index mapping versions) in cluster state.
 * Transport version from pre-8.8.0 cluster state was inferred as the minimum possible,
 * due to the master node not understanding cluster state with transport versions added in 8.8.0.
 * Any nodes with the inferred placeholder cluster state is then refreshed with their actual transport version.
 * Same for system index mapping versions: when upgraded from pre-8.11.0, cluster state holds and empty map of system index mapping
 * versions. Any nodes with an empty system index mapping versions map in cluster state is refreshed with their actual versions.
 */
public class CompatibilityVersionsFixupListener implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(CompatibilityVersionsFixupListener.class);

    static final NodeFeature FIX_TRANSPORT_VERSION = new NodeFeature("transport.fix_transport_version");

    private static final TimeValue RETRY_TIME = TimeValue.timeValueSeconds(30);

    private final MasterServiceTaskQueue<NodeCompatibilityVersionsTask> taskQueue;
    private final ClusterAdminClient client;
    private final Scheduler scheduler;
    private final Executor executor;
    private final Set<String> pendingNodes = Collections.synchronizedSet(new HashSet<>());
    private final FeatureService featureService;

    public CompatibilityVersionsFixupListener(
        ClusterService service,
        ClusterAdminClient client,
        FeatureService featureService,
        ThreadPool threadPool
    ) {
        // there tends to be a lot of state operations on an upgrade - this one is not time-critical,
        // so use LOW priority. It just needs to be run at some point after upgrade.
        this(
            service.createTaskQueue("fixup-transport-versions", Priority.LOW, new TransportVersionUpdater()),
            client,
            featureService,
            threadPool,
            threadPool.executor(ThreadPool.Names.CLUSTER_COORDINATION)
        );
    }

    CompatibilityVersionsFixupListener(
        MasterServiceTaskQueue<NodeCompatibilityVersionsTask> taskQueue,
        ClusterAdminClient client,
        FeatureService featureService,
        Scheduler scheduler,
        Executor executor
    ) {
        this.taskQueue = taskQueue;
        this.client = client;
        this.featureService = featureService;
        this.scheduler = scheduler;
        this.executor = executor;
    }

    class NodeCompatibilityVersionsTask implements ClusterStateTaskListener {
        private final Map<String, CompatibilityVersions> results;
        private final int retryNum;

        NodeCompatibilityVersionsTask(Map<String, CompatibilityVersions> results, int retryNum) {
            this.results = results;
            this.retryNum = retryNum;
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Could not apply compatibility versions for nodes {} to cluster state", results.keySet(), e);
            scheduleRetry(results.keySet(), retryNum);
        }

        public Map<String, CompatibilityVersions> results() {
            return results;
        }
    }

    static class TransportVersionUpdater implements ClusterStateTaskExecutor<NodeCompatibilityVersionsTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<NodeCompatibilityVersionsTask> context) throws Exception {
            ClusterState.Builder builder = ClusterState.builder(context.initialState());
            boolean modified = false;
            for (var c : context.taskContexts()) {
                for (var e : c.getTask().results().entrySet()) {
                    // this node's transport version might have been updated already/node has gone away
                    var cvMap = builder.compatibilityVersions();
                    var currentCompatibilityVersions = cvMap.get(e.getKey());

                    TransportVersion recordedTv = Optional.ofNullable(currentCompatibilityVersions)
                        .map(CompatibilityVersions::transportVersion)
                        .orElse(null);

                    assert (currentCompatibilityVersions != null) || (context.initialState().nodes().nodeExists(e.getKey()) == false)
                        : "Node " + e.getKey() + " is in the cluster but does not have an associated transport version recorded";

                    var systemIndexMappingsVersion = Optional.ofNullable(currentCompatibilityVersions)
                        .map(CompatibilityVersions::systemIndexMappingsVersion)
                        .orElse(Map.of());

                    if (Objects.equals(recordedTv, INFERRED_TRANSPORT_VERSION) || systemIndexMappingsVersion.isEmpty()) {
                        builder.putCompatibilityVersions(
                            e.getKey(),
                            e.getValue().transportVersion(),
                            e.getValue().systemIndexMappingsVersion()
                        );
                        modified = true;
                    }
                }
                c.success(() -> {});
            }
            return modified ? builder.build() : context.initialState();
        }
    }

    @SuppressForbidden(reason = "maintaining ClusterState#compatibilityVersions requires reading them")
    private static Map<String, CompatibilityVersions> getCompatibilityVersions(ClusterState clusterState) {
        return clusterState.compatibilityVersions();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) return; // only if we're master

        List<Stream<String>> queries = new ArrayList<>();
        Map<String, CompatibilityVersions> compatibilityVersions = getCompatibilityVersions(event.state());

        // if the min node version > 8.8.0, and the cluster state has some transport versions == 8.8.0,
        // then refresh all inferred transport versions to their real versions
        // now that everything should understand cluster state with transport versions
        if (featureService.clusterHasFeature(event.state(), FIX_TRANSPORT_VERSION)
            && event.state().getMinTransportVersion().equals(INFERRED_TRANSPORT_VERSION)) {

            // find all the relevant nodes
            queries.add(
                compatibilityVersions.entrySet()
                    .stream()
                    .filter(e -> e.getValue().transportVersion().equals(INFERRED_TRANSPORT_VERSION))
                    .map(Map.Entry::getKey)
            );
        }

        /*
         * Also check if there are any nodes that should have SystemIndex mapping versions in cluster state, but don't.
         * This can happen if the master was upgraded from before 8.11, and one or more non-master nodes
         * were already upgraded. They don't re-join the cluster with the new master, so never get their CompatibilityVersions
         * updated into cluster state.
         * So we need to do a separate transport call to get the node SystemIndex mapping versions and add them to cluster state.
         * We can't use features to determine when this should happen, as that is unreliable for upgrades (same issue,
         * see NodeFeaturesFixupListener).
         * We also can't use transport version, as that is unreliable for upgrades from versions before 8.8
         * (see TransportVersionFixupListener).
         * So the only thing we can use is release version. This is ok here, as Serverless will never hit this case, so the node
         * feature fetch action will never be called on Serverless.
         * The problem affects 8.11+, but as we fixed the problem in 8.16.1 and 8.17.0 (by adding systemIndexMappingsVersion to
         * NodesInfoResponse), we can fix only nodes with version > 8.16.0.
         * This whole class will be removed in ES v9.
         */
        queries.add(
            event.state()
                .nodes()
                .stream()
                .filter(n -> n.getVersion().after(Version.V_8_16_0))
                .map(DiscoveryNode::getId)
                .filter(n -> compatibilityVersions.getOrDefault(n, CompatibilityVersions.EMPTY).systemIndexMappingsVersion().isEmpty())
        );

        Set<String> queryNodes = queries.stream().flatMap(Function.identity()).collect(Collectors.toSet());

        if (queryNodes.isEmpty() == false) {
            logger.debug("Fetching actual compatibility versions for nodes {}", queryNodes);
            updateTransportVersions(queryNodes, 0);
        }
    }

    private void scheduleRetry(Set<String> nodes, int thisRetryNum) {
        // just keep retrying until this succeeds
        logger.debug("Scheduling retry {} for nodes {}", thisRetryNum + 1, nodes);
        scheduler.schedule(() -> updateTransportVersions(nodes, thisRetryNum + 1), RETRY_TIME, executor);
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
                logger.warn("Could not read nodes info for nodes {}", outstandingNodes, e);
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

        Map<String, CompatibilityVersions> results = response.getNodes()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    n -> n.getNode().getId(),
                    n -> new CompatibilityVersions(n.getTransportVersion(), n.getCompatibilityVersions())
                )
            );

        if (results.isEmpty() == false) {
            taskQueue.submitTask("update-transport-version", new NodeCompatibilityVersionsTask(results, retryNum), null);
        }
    }
}
