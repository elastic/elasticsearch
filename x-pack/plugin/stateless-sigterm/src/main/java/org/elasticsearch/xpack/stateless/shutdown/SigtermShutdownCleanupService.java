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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.SIGTERM;
import static org.elasticsearch.core.Strings.format;

/**
 * Cleans up expired {@link SingleNodeShutdownMetadata} of type {@link SingleNodeShutdownMetadata.Type#SIGTERM} from cluster state.
 */
public class SigtermShutdownCleanupService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(SigtermShutdownCleanupService.class);
    private static final long GRACE_PERIOD_SAFETY_DIVISOR = 10;
    static final Scheduler.Cancellable PENDING_DELAYED_CLEANUP = new CleanupMarker();
    static final Scheduler.Cancellable IN_FLIGHT_CLEANUP = new CleanupMarker();
    private final ThreadPool threadPool;
    private final Executor executor;
    private final MasterServiceTaskQueue<CleanupSigtermShutdownTask> taskQueue;

    record Node(String id, String ephemeralId) {}

    final ConcurrentHashMap<Node, Scheduler.Cancellable> cleanups = new ConcurrentHashMap<>();

    public SigtermShutdownCleanupService(ClusterService clusterService, RerouteService rerouteService) {
        this.threadPool = clusterService.threadPool();
        this.executor = threadPool.generic();
        this.taskQueue = clusterService.createTaskQueue(
            "shutdown-sigterm-cleaner",
            Priority.NORMAL,
            new RemoveSigtermShutdownTaskExecutor(rerouteService)
        );
    }

    /**
     * Schedule a {@link SubmitCleanupSigtermShutdown} action when there is a sigterm {@link SingleNodeShutdownMetadata} in the cluster
     * state.  Actions are scheduled based on the started time of the shutdown and the grace period.
     * See {@link #computeDelay(long, long, long)} for details in the calculation.  The action is rescheduled
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().isLocalNodeElectedMaster() == false) {
            // Only do this if we're the current master node.
            return;
        }

        NodesShutdownMetadata eventShutdownMetadata = event.state().metadata().custom(NodesShutdownMetadata.TYPE);

        if (eventShutdownMetadata == null) {
            return;
        }

        // Only fetch the time if necessary
        long now = Long.MIN_VALUE;

        for (SingleNodeShutdownMetadata shutdown : eventShutdownMetadata.getAll().values()) {
            if (shutdown.getType() == SIGTERM) {
                var node = new Node(shutdown.getNodeId(), shutdown.getNodeEphemeralId());
                // First check if the node being shutdown is still running. If it has a different
                // ephemeral id than when it was marked for shutdown, then we can remove the shutdown record immediately.
                DiscoveryNode discoveryNode = event.state().nodes().get(shutdown.getNodeId());
                if (discoveryNode != null
                    && shutdown.getNodeEphemeralId() != null
                    && discoveryNode.getEphemeralId().equals(shutdown.getNodeEphemeralId()) == false) {
                    logger.debug(
                        format(
                            "Node [%s] with ephemeral id [%s] has new ephemeral id [%s]. Removing shutdown record.",
                            discoveryNode.getId(),
                            shutdown.getNodeEphemeralId(),
                            discoveryNode.getEphemeralId()
                        )
                    );
                    // the node has a different ephemeral id, so we can remove this shutdown record immediately
                    submitCleanupTask("sigterm-shutdown-restarted", node);
                    continue;
                }

                // The node still has the same ephemeral id, so we proceed with the normal cleanup after
                // the grace period has expired.

                final Scheduler.Cancellable cleanup = cleanups.get(node);
                // Skip nodes that already have an active delayed cleanup or an in-flight master task.
                if (cleanup == PENDING_DELAYED_CLEANUP
                    || cleanup == IN_FLIGHT_CLEANUP
                    || (cleanup != null && cleanup.isCancelled() == false)) {
                    continue;
                }
                // If the cancelled entry was concurrently replaced, leave it alone and do not reschedule.
                if (cleanup != null && cleanups.remove(node, cleanup) == false) {
                    continue;
                }

                if (now == Long.MIN_VALUE) {
                    now = threadPool.absoluteTimeInMillis();
                }

                // Reserve this node before scheduling to avoid schedule-then-cancel churn under repeated cluster state updates.
                if (cleanups.putIfAbsent(node, PENDING_DELAYED_CLEANUP) != null) {
                    continue;
                }
                final Scheduler.ScheduledCancellable scheduledCleanup;
                try {
                    scheduledCleanup = threadPool.schedule(
                        new SubmitCleanupSigtermShutdown(
                            node,
                            cleanupNode -> submitCleanupTask("sigterm-grace-period-expired", cleanupNode)
                        ),
                        computeDelay(now, shutdown.getStartedAtMillis(), shutdown.getGracePeriod().millis()),
                        executor
                    );
                } catch (RuntimeException e) {
                    cleanups.remove(node, PENDING_DELAYED_CLEANUP);
                    throw e;
                }
                if (cleanups.replace(node, PENDING_DELAYED_CLEANUP, scheduledCleanup) == false) {
                    scheduledCleanup.cancel();
                }
            }
        }
    }

    private void submitCleanupTask(String source, Node node) {
        final Scheduler.Cancellable existingCleanup = cleanups.put(node, IN_FLIGHT_CLEANUP);
        if (existingCleanup == IN_FLIGHT_CLEANUP) {
            logger.trace(() -> format("cleanup task already in-flight for node [%s]", node));
            return;
        }
        if (existingCleanup != null) {
            existingCleanup.cancel();
        }
        taskQueue.submitTask(source, new CleanupSigtermShutdownTask(node, cleanups::remove), null);
    }

    /**
     * The amount of time to wait until the {@param grace} has expired, plus a little extra for safety,
     * {@link #GRACE_PERIOD_SAFETY_DIVISOR}.  {@param now} is the current time and {@param started} is
     * when the shutdown was first seen in cluster state.  All times in milliseconds.
     * If, due to clock skew, {@param started} is in the future, the elapsed time is clamped to zero.
     * Never returns a negative {#link TimeValue}.
     */
    static TimeValue computeDelay(long now, long started, long grace) {
        long elapsed = now - started;
        if (elapsed < 0) {
            elapsed = 0;
        }
        long delay = (grace + grace / GRACE_PERIOD_SAFETY_DIVISOR) - elapsed;
        if (delay <= 0) {
            return TimeValue.ZERO;
        }
        return new TimeValue(delay);
    }

    /**
     * Collection of state necessary to submit a {@link CleanupSigtermShutdownTask}.
     */
    record SubmitCleanupSigtermShutdown(Node node, Consumer<Node> submitCleanup) implements Runnable {
        SubmitCleanupSigtermShutdown {
            Objects.requireNonNull(node);
            Objects.requireNonNull(submitCleanup);
        }

        @Override
        public void run() {
            submitCleanup.accept(node);
        }
    }

    record CleanupSigtermShutdownTask(Node node, Consumer<Node> onProcessed) implements ClusterStateTaskListener {
        CleanupSigtermShutdownTask {
            Objects.requireNonNull(node);
            Objects.requireNonNull(onProcessed);
        }

        void markProcessed() {
            onProcessed.accept(node);
        }

        @Override
        public void onFailure(Exception e) {
            markProcessed();
            logger.warn(() -> format("failed to cleanup sigterm shutdown metadata for node [%s]", node), e);
        }
    }

    static class RemoveSigtermShutdownTaskExecutor implements ClusterStateTaskExecutor<CleanupSigtermShutdownTask> {

        private final RerouteService rerouteService;

        RemoveSigtermShutdownTaskExecutor(RerouteService rerouteService) {
            this.rerouteService = rerouteService;
        }

        @Override
        public ClusterState execute(BatchExecutionContext<CleanupSigtermShutdownTask> batchExecutionContext) throws Exception {
            ClusterState state = cleanupSigtermShutdowns(
                batchExecutionContext.taskContexts()
                    .stream()
                    .map(ClusterStateTaskExecutor.TaskContext::getTask)
                    .map(CleanupSigtermShutdownTask::node)
                    .collect(Collectors.toUnmodifiableSet()),
                batchExecutionContext.initialState()
            );
            final Runnable doReroute = state == batchExecutionContext.initialState()
                ? () -> {}
                : new RunOnce(() -> rerouteService.reroute("after removing shutdown marker", Priority.NORMAL, ActionListener.noop()));
            batchExecutionContext.taskContexts().forEach(taskContext -> taskContext.success(() -> {
                doReroute.run();
                taskContext.getTask().markProcessed();
            }));
            return state;
        }

        /**
         * Remove the {@link SingleNodeShutdownMetadata} of type SIGTERM for all {@param nodes} that are no longer in the cluster.
         */
        static ClusterState cleanupSigtermShutdowns(Set<Node> nodes, ClusterState initialState) {
            var shutdownMetadata = new HashMap<>(initialState.metadata().nodeShutdowns().getAll());

            boolean modified = false;
            for (Node node : nodes) {
                DiscoveryNode discoveryNode = initialState.nodes().get(node.id);
                if (discoveryNode != null && discoveryNode.getEphemeralId().equals(node.ephemeralId)) {
                    logger.warn(format("cannot remove sigterm shutdown for node [%s] that has not left the cluster", node));
                } else {
                    SingleNodeShutdownMetadata singleShutdown = shutdownMetadata.remove(node.id);
                    if (singleShutdown == null) {
                        // Could happen if, for example, we've received a cluster state update after task submission but before shutdown
                        // removal.
                        logger.trace(() -> format("sigterm shutdown already removed for node [%s]", node));
                    } else if (singleShutdown.getType() != SIGTERM) {
                        logger.warn(
                            format(
                                "not removing unexpected shutdown type [%s] for node [%s], expected SIGTERM",
                                singleShutdown.getType(),
                                node
                            )
                        );
                        // this is not the shutdown we are looking for
                        shutdownMetadata.put(node.id, singleShutdown);
                    } else if (Objects.equals(singleShutdown.getNodeEphemeralId(), node.ephemeralId) == false) {
                        logger.warn(
                            format(
                                "not removing sigterm shutdown for node [%s], expected ephemeral id [%s]",
                                node,
                                singleShutdown.getNodeEphemeralId()
                            )
                        );
                        shutdownMetadata.put(node.id, singleShutdown);
                    } else {
                        modified = true;
                    }
                }
            }

            if (modified == false) {
                return initialState;
            }

            return ClusterState.builder(initialState)
                .metadata(
                    Metadata.builder(initialState.metadata())
                        .putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdownMetadata))
                        .build()
                )
                .build();
        }
    }

    private static class CleanupMarker implements Scheduler.Cancellable {
        @Override
        public boolean cancel() {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
    }
}
