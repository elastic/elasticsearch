/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link DelayedAllocationService} listens to cluster state changes and checks
 * if there are unassigned shards with delayed allocation (unassigned shards that have
 * the delay marker). These are shards that have become unassigned due to a node leaving
 * and which were assigned the delay marker based on the index delay setting
 * {@link UnassignedInfo#INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING}
 * (see {@link AllocationService#disassociateDeadNodes(RoutingAllocation)}.
 * This class is responsible for choosing the next (closest) delay expiration of a
 * delayed shard to schedule a reroute to remove the delay marker.
 * The actual removal of the delay marker happens in
 * {@link AllocationService#removeDelayMarkers(RoutingAllocation)}, triggering yet
 * another cluster change event.
 */
public class DelayedAllocationService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(DelayedAllocationService.class);

    static final String CLUSTER_UPDATE_TASK_SOURCE = "delayed_allocation_reroute";

    final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final AllocationService allocationService;

    AtomicReference<DelayedRerouteTask> delayedRerouteTask = new AtomicReference<>(); // package private to access from tests

    /**
     * represents a delayed scheduling of the reroute action that can be cancelled.
     */
    class DelayedRerouteTask extends ClusterStateUpdateTask {
        final TimeValue nextDelay; // delay until submitting the reroute command
        final long baseTimestampNanos; // timestamp (in nanos) upon which delay was calculated
        volatile Scheduler.Cancellable cancellable;
        final AtomicBoolean cancelScheduling = new AtomicBoolean();

        DelayedRerouteTask(TimeValue nextDelay, long baseTimestampNanos) {
            this.nextDelay = nextDelay;
            this.baseTimestampNanos = baseTimestampNanos;
        }

        public long scheduledTimeToRunInNanos() {
            return baseTimestampNanos + nextDelay.nanos();
        }

        public void cancelScheduling() {
            cancelScheduling.set(true);
            if (cancellable != null) {
                cancellable.cancel();
            }
            removeIfSameTask(this);
        }

        public void schedule() {
            cancellable = threadPool.schedule(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    if (cancelScheduling.get()) {
                        return;
                    }
                    clusterService.submitStateUpdateTask(CLUSTER_UPDATE_TASK_SOURCE, DelayedRerouteTask.this, newExecutor());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to submit schedule/execute reroute post unassigned shard", e);
                    removeIfSameTask(DelayedRerouteTask.this);
                }
            }, nextDelay, ThreadPool.Names.SAME);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            removeIfSameTask(this);
            return allocationService.reroute(currentState, "assign delayed unassigned shards");
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            if (oldState == newState) {
                // no state changed, check when we should remove the delay flag from the shards the next time.
                // if cluster state changed, we can leave the scheduling of the next delay up to the clusterChangedEvent
                // this should not be needed, but we want to be extra safe here
                scheduleIfNeeded(currentNanoTime(), newState);
            }
        }

        @Override
        public void onFailure(Exception e) {
            removeIfSameTask(this);
            logger.warn("failed to schedule/execute reroute post unassigned shard", e);
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> newExecutor() {
        return ClusterStateTaskExecutor.unbatched();
    }

    @Inject
    public DelayedAllocationService(ThreadPool threadPool, ClusterService clusterService, AllocationService allocationService) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        if (DiscoveryNode.isMasterNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {
        clusterService.removeListener(this);
        removeTaskAndCancel();
    }

    /** override this to control time based decisions during delayed allocation */
    protected long currentNanoTime() {
        return System.nanoTime();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            long currentNanoTime = currentNanoTime();
            scheduleIfNeeded(currentNanoTime, event.state());
        }
    }

    private void removeTaskAndCancel() {
        DelayedRerouteTask existingTask = delayedRerouteTask.getAndSet(null);
        if (existingTask != null) {
            logger.trace("cancelling existing delayed reroute task");
            existingTask.cancelScheduling();
        }
    }

    private void removeIfSameTask(DelayedRerouteTask expectedTask) {
        delayedRerouteTask.compareAndSet(expectedTask, null);
    }

    /**
     * Figure out if an existing scheduled reroute is good enough or whether we need to cancel and reschedule.
     */
    private synchronized void scheduleIfNeeded(long currentNanoTime, ClusterState state) {
        assertClusterOrMasterStateThread();
        long nextDelayNanos = UnassignedInfo.findNextDelayedAllocation(currentNanoTime, state);
        if (nextDelayNanos < 0) {
            logger.trace("no need to schedule reroute - no delayed unassigned shards");
            removeTaskAndCancel();
        } else {
            TimeValue nextDelay = TimeValue.timeValueNanos(nextDelayNanos);
            final boolean earlierRerouteNeeded;
            DelayedRerouteTask existingTask = delayedRerouteTask.get();
            DelayedRerouteTask newTask = new DelayedRerouteTask(nextDelay, currentNanoTime);
            if (existingTask == null) {
                earlierRerouteNeeded = true;
            } else if (newTask.scheduledTimeToRunInNanos() < existingTask.scheduledTimeToRunInNanos()) {
                // we need an earlier delayed reroute
                logger.trace(
                    "cancelling existing delayed reroute task as delayed reroute has to happen [{}] earlier",
                    TimeValue.timeValueNanos(existingTask.scheduledTimeToRunInNanos() - newTask.scheduledTimeToRunInNanos())
                );
                existingTask.cancelScheduling();
                earlierRerouteNeeded = true;
            } else {
                earlierRerouteNeeded = false;
            }

            if (earlierRerouteNeeded) {
                logger.info(
                    "scheduling reroute for delayed shards in [{}] ({} delayed shards)",
                    nextDelay,
                    UnassignedInfo.getNumberOfDelayedUnassigned(state)
                );
                DelayedRerouteTask currentTask = delayedRerouteTask.getAndSet(newTask);
                assert existingTask == currentTask || currentTask == null;
                newTask.schedule();
            } else {
                logger.trace("no need to reschedule delayed reroute - currently scheduled delayed reroute in [{}] is enough", nextDelay);
            }
        }
    }

    // protected so that it can be overridden (and disabled) by unit tests
    protected void assertClusterOrMasterStateThread() {
        assert ClusterService.assertClusterOrMasterStateThread();
    }
}
