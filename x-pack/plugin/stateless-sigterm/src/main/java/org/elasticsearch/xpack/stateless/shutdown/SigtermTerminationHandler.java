/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.readiness.ReadinessRequest;
import org.elasticsearch.readiness.ReadinessService;
import org.elasticsearch.readiness.TransportReadinessAction;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportClient;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.shutdown.GetShutdownStatusAction;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.SingleNodeShutdownStatus;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.elasticsearch.action.support.master.MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT;
import static org.elasticsearch.core.TimeValue.timeValueMillis;

/**
 * This class actually implements the logic that's invoked when Elasticsearch receives a sigterm - that is, issuing a Put Shutdown request
 * for this node and periodically checking the Get Shutdown Status API for this node until it's done.
 */
public class SigtermTerminationHandler implements TerminationHandler {
    private static final Logger logger = LogManager.getLogger(SigtermTerminationHandler.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final RemoteTransportClient remoteTransportClient;
    private final TimeValue pollInterval;
    private final TimeValue timeout;
    private final String nodeId;

    public SigtermTerminationHandler(
        Client client,
        ThreadPool threadPool,
        ClusterService clusterService,
        RemoteTransportClient remoteTransportClient,
        TimeValue pollInterval,
        TimeValue timeout,
        String nodeId
    ) {
        this.client = new OriginSettingClient(client, ClientHelper.STACK_ORIGIN);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.remoteTransportClient = remoteTransportClient;
        this.pollInterval = pollInterval;
        this.timeout = timeout;
        this.nodeId = nodeId;
    }

    @Override
    public void handleTermination() {
        logger.info("handling graceful shutdown request");
        final long started = threadPool.rawRelativeTimeInMillis();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SingleNodeShutdownStatus> lastStatus = new AtomicReference<>();
        AtomicBoolean failed = new AtomicBoolean(false);
        client.execute(
            PutShutdownNodeAction.INSTANCE,
            shutdownRequest(),
            ActionListener.wrap(res -> pollStatusAndLoop(0, latch, lastStatus), ex -> {
                logger.warn("failed to register graceful shutdown request, stopping immediately", ex);
                failed.set(true);
                latch.countDown();
            })
        );
        try {
            boolean latchReachedZero = latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
            boolean timedOut = latchReachedZero == false && timeout.millis() != 0;
            SingleNodeShutdownStatus status = lastStatus.get();
            if (timedOut && status != null && status.migrationStatus().getShardsRemaining() > 0) {
                logger.info("Timed out waiting for graceful shutdown, retrieving current recoveries status");
                logDetailedRecoveryStatusAndWait();
            }
            var duration = threadPool.rawRelativeTimeInMillis() - started;
            logger.info(
                new ESLogMessage("shutdown completed after [{}] ms with status [{}]", duration, status) //
                    .withFields(
                        Map.of(
                            "elasticsearch.shutdown.status",
                            getShutdownStatus(failed.get(), status),
                            "elasticsearch.shutdown.duration",
                            duration,
                            "elasticsearch.shutdown.timed-out",
                            timedOut
                        )
                    )
            );
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void blockTermination() {
        // The kubernetes descheduler can shut down pods. For deployments having just a single
        // pod of a given type, the pod being shut down does not yet have a replacement ready.
        // To reduce the probability and duration of downtime, we hold off actually beginning shutdown
        // until a replacement pod with the same role is ready.

        DiscoveryNode localNode = clusterService.state().nodes().getLocalNode();
        if (localNode == null) {
            logger.info("No local node yet; continuing with shutdown");
            return;
        }

        if (ReadinessService.PORT.get(clusterService.getSettings()) == -1) {
            // If the readiness port is not configured, we take that as an indication that
            // readiness checking is not desired. Strictly speaking,
            // it's actually more appropriate to check a node's readiness if _its_ readiness service is configured;
            // but in real clusters, we'd expect readiness to be configured uniformly across the nodes anyway,
            // and this check is simpler, quicker, more deterministic, and more fault-tolerant
            // than somehow trying to detect whether the remote node has its readiness service configured.
            //
            // If we don't do this check at all, then clusters with readiness checking disabled
            // can hang at shutdown waiting for a readiness-enabled node that will never arrive.
            logger.info("Readiness port not configured; continuing with shutdown without blocking");
            return;
        }

        // Note that waiting for all the same roles as the current node would mean we could never remove roles.
        // Instead, these roles are expected to be disjoint; stateless nodes do not share them, so we find
        // the one relevant role of the current node.
        Set<DiscoveryNodeRole> relevantRoles = Set.of(
            DiscoveryNodeRole.INDEX_ROLE,
            DiscoveryNodeRole.SEARCH_ROLE,
            DiscoveryNodeRole.ML_ROLE
        );
        Set<DiscoveryNodeRole> currentRoles = localNode.getRoles();
        Set<DiscoveryNodeRole> foundRoles = localNode.getRoles().stream().filter(relevantRoles::contains).collect(Collectors.toSet());
        if (foundRoles.isEmpty()) {
            assert false : "Expected all stateless nodes to have exactly one role from " + relevantRoles;
            logger.warn(
                "Current node roles "
                    + currentRoles
                    + " do not contain any relevant roles in "
                    + relevantRoles
                    + " to block on. Continuing with shutdown."
            );
            return;
        } else if (foundRoles.size() > 1) {
            assert false : "Expected all stateless nodes to have exactly one role from " + relevantRoles;
            logger.warn(
                "Current node roles "
                    + currentRoles
                    + " contain more than one role from "
                    + relevantRoles
                    + " but expected these roles to be disjoint"
            );
            return;
        }
        DiscoveryNodeRole targetRole = foundRoles.iterator().next();
        if (targetRole == DiscoveryNodeRole.ML_ROLE) {
            logger.info("Node with role {} is allowed to scale to zero; proceeding with shutdown", targetRole);
            return;
        }
        logger.info("Blocking shutdown of node {} waiting for a node with role {}", localNode.getId(), targetRole);
        Predicate<DiscoveryNode> nodeFilter = node -> node.getId().equals(nodeId) == false && node.getRoles().contains(targetRole);

        ReadyChecker readyChecker = new ReadyChecker();

        // Watch for new nodes to join
        ClusterStateListener nodesChangedListener = event -> {
            if (event.nodesChanged() == false) {
                return;
            }
            readyChecker.setCurrentNodes(event.state().nodes().stream());
            Set<DiscoveryNode> addedNodes = event.nodesDelta().addedNodes().stream().filter(nodeFilter).collect(Collectors.toSet());
            readyChecker.sendReadyChecks(addedNodes);
        };
        clusterService.addListener(nodesChangedListener);

        // Immediately check any nodes already in the cluster
        Set<DiscoveryNode> nodes = clusterService.state().nodes().stream().filter(nodeFilter).collect(Collectors.toSet());
        readyChecker.setCurrentNodes(nodes.stream());
        readyChecker.sendReadyChecks(nodes);

        // Now block and wait for the first ready response
        try {
            logger.debug("Waiting for replacement node");
            readyChecker.readyLatch.await();
            logger.info("Replacement node is ready; proceeding with shutdown");
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for nodes to be ready, continuing with shutdown.");
            Thread.currentThread().interrupt();
            assert false : "Shutdown thread interrupted";
        }
        clusterService.removeListener(nodesChangedListener);
    }

    /**
     * Tracks the readiness checks currently in flight and indicates when at least one has succeeded.
     */
    private class ReadyChecker {
        /**
         * IDs of nodes for which there's already a request loop in progress.
         * (Nodes that successfully indicate readiness are not removed from here,
         * since there's never any point making another request to such a node; we're done!)
         */
        private final Set<String> pendingNodes = new HashSet<>();

        /**
         * Null indicates we don't yet have information about the nodes in the cluster
         * and should not conclude that we can stop the retry loop for any particular node.
         * This happens during the brief window after we've registered the cluster state listener
         * but before we've fetched the node list for the initial round of checks.
         * We'll set this later, once things have settled, and from then on it can be used reliably
         * to stop retries on nodes.
         */
        @Nullable
        private volatile Set<String> currentNodes = null;

        private void setCurrentNodes(Stream<DiscoveryNode> nodes) {
            this.currentNodes = nodes.map(DiscoveryNode::getId).collect(Collectors.toSet());
        }

        /**
         * Indicates when at least one suitable node is ready and termination can proceed
         */
        final CountDownLatch readyLatch = new CountDownLatch(1);

        /**
         * Ensures there are ready checks pending for all {@code suitableNodes}
         * that are in the cluster
         * without kicking off redundant checks for the same node.
         * <p>
         * {@code suitableNodes} may (redundantly) contain nodes we're already watching.
         *
         * @param suitableNodes nodes to which we want to ensure a readiness check is in progress.
         */
        void sendReadyChecks(Set<DiscoveryNode> suitableNodes) {
            // We want to retry this on a timescale of pollInterval, but also want to respond promptly
            // if the node becomes ready much sooner than that, so we do an initial exponential backoff
            // until we hit pollInterval.
            TimeValue initialRetryInterval = timeValueMillis(max(pollInterval.millis() / 32, 1));

            for (DiscoveryNode node : suitableNodes) {
                if (pendingNodes.add(node.getId())) {
                    maybeSendReadyCheck(node, initialRetryInterval);
                } else {
                    logger.debug("Node {} already has a pending readiness check", node.getId());
                }
            }
        }

        /**
         * Sends a {@link ReadinessRequest} to {@code node}, retrying with exponential backoff on error.
         * For nodes removed from the cluster, we quit the retry loop and remove the node from {@link #pendingNodes}
         * in case it re-joins in the future.
         */
        private void maybeSendReadyCheck(DiscoveryNode node, TimeValue retryInterval) {
            var currentNodes = this.currentNodes;
            if (currentNodes != null && currentNodes.contains(node.getId()) == false) {
                logger.debug("Node {} no longer exists in the cluster", node.getId());
                // Remove the node from pendingNodes so that we start up a fresh request loop if it ever rejoins
                pendingNodes.remove(node.getId());
                return;
            }

            try (var ignored = threadPool.getThreadContext().newEmptySystemContext()) {
                logger.debug("Sending readiness check to node {}", node.getId());

                remoteTransportClient.sendRequest(
                    node,
                    TransportReadinessAction.TYPE.name(),
                    new ReadinessRequest(),
                    new TransportResponseHandler.Empty() {
                        @Override
                        public Executor executor() {
                            return TransportResponseHandler.TRANSPORT_WORKER;
                        }

                        @Override
                        public void handleResponse() {
                            logger.debug("Node " + node.getId() + " is ready, continuing with shutdown");
                            readyLatch.countDown();
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.warn("Readiness check request failed", exp);
                            if (readyLatch.getCount() > 0) {
                                logger.debug("Will retry in {}", retryInterval);
                                TimeValue nextRetryInterval = timeValueMillis(min(2 * retryInterval.millis(), pollInterval.millis()));
                                threadPool.schedule(
                                    () -> maybeSendReadyCheck(node, nextRetryInterval),
                                    retryInterval,
                                    threadPool.generic()
                                );
                            }
                        }
                    }
                );

                logger.debug("Sent readiness check for node {}", node.getId());
            }
        }

    }

    private static String getShutdownStatus(boolean failed, SingleNodeShutdownStatus status) {
        if (failed) {
            return "FAILED";
        } else if (status != null) {
            return status.overallStatus().toString();
        } else {
            return "UNKNOWN";
        }
    }

    private void logDetailedRecoveryStatusAndWait() {
        CountDownLatch latch = new CountDownLatch(1);
        logDetailedRecoveryStatus(latch::countDown);
        try {
            latch.await(); // no need for a timeout, if this takes too long the node will shutdown anyways
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void logDetailedRecoveryStatus(Releasable releasable) {
        var request = new RecoveryRequest();
        request.activeOnly(true);
        client.execute(RecoveryAction.INSTANCE, request, ActionListener.releaseAfter(new ActionListener<>() {
            @Override
            public void onResponse(RecoveryResponse recoveryResponse) {
                logger.warn("Ongoing recoveries: {}", recoveryResponse);
            }

            @Override
            public void onFailure(Exception ex) {
                logger.error("Failed to get recoveries status", ex);
            }
        }, releasable));
    }

    private PutShutdownNodeAction.Request shutdownRequest() {
        PutShutdownNodeAction.Request request = new PutShutdownNodeAction.Request(
            timeout,
            timeout,
            nodeId,
            SingleNodeShutdownMetadata.Type.SIGTERM,
            "node sigterm",
            null,
            null,
            timeout
        );
        assert request.validate() == null;
        return request;
    }

    private void pollStatusAndLoop(int poll, CountDownLatch latch, AtomicReference<SingleNodeShutdownStatus> lastStatus) {
        // This transport action does not use a timeout, so we use INFINITE_MASTER_NODE_TIMEOUT as a way to express "this will not time out"
        final var request = new GetShutdownStatusAction.Request(INFINITE_MASTER_NODE_TIMEOUT, nodeId);
        client.execute(GetShutdownStatusAction.INSTANCE, request, ActionListener.wrap(res -> {
            assert res.getShutdownStatuses().size() == 1 : "got more than this node's shutdown status";
            SingleNodeShutdownStatus status = res.getShutdownStatuses().get(0);
            lastStatus.set(status);
            if (status.overallStatus().equals(SingleNodeShutdownMetadata.Status.COMPLETE)) {
                logger.debug("node ready for shutdown with status [{}]: {}", status.overallStatus(), status);
                latch.countDown();
            } else {
                final var level = poll % 10 == 0 ? Level.INFO : Level.DEBUG;
                if (logger.isEnabled(level)) {
                    logger.log(
                        level,
                        new ESLogMessage("polled for shutdown status: {}", status).withFields(
                            Map.of("elasticsearch.shutdown.poll_count", poll)
                        )
                    );
                    if (poll > 0) {
                        HotThreads.logLocalHotThreads(
                            logger,
                            level,
                            "hot threads while waiting for shutdown [poll_count=" + poll + "]",
                            ReferenceDocs.LOGGING
                        );
                        logDetailedRecoveryStatus(() -> {});
                    }
                }
                threadPool.schedule(() -> pollStatusAndLoop(poll + 1, latch, lastStatus), pollInterval, threadPool.generic());
            }
        }, ex -> {
            // if the node times out while waiting for a graceful shutdown, it's likely that the last GetShutdownStatusAction
            // invocation will fail with a NodeClosedException. Logging in this case is not necessary (we already timed out and started
            // the immediate shutdown process) and could lead to a spurious entry in our dashboards, so let's skip it.
            if (ex instanceof NodeClosedException == false) {
                logger.warn("failed to get shutdown status for this node while waiting for shutdown, stopping immediately", ex);
            }
            latch.countDown();
        }));
    }
}
