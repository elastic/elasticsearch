
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

package co.elastic.elasticsearch.stateless.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Service that runs on the elected master node in order to remove the IndexMetadata.INDEX_REFRESH_BLOCK index block from cluster state.
 *
 * The service uses a {@link ClusterStateListener} to detect newly created indices with refresh blocks and add them, along with the
 * current time in milliseconds, to a queue of blocked indices. It then schedules a task to process the queue, which consists of removing
 * entries from the queue if their timestamps + the value of the {@link #EXPIRE_AFTER_SETTING} setting indicate that the refresh block is
 * expired. If one or more indices have a block expired, a cluster state update task is submitted to remove the blocks.
 *
 * The service also removes refresh blocks for indices that have been created with a refresh block but later updated to have no replicas
 * before the block reached expiration. In those cases the index refresh block is removed immediately from cluster state.
 */
public class RemoveRefreshClusterBlockService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(RemoveRefreshClusterBlockService.class);

    /**
     * How long a refresh block should remain in cluster state.
     */
    public static final Setting<TimeValue> EXPIRE_AFTER_SETTING = Setting.timeSetting(
        "stateless.cluster.refresh_blocks.expire_after",
        TimeValue.timeValueSeconds(30L), // default
        TimeValue.timeValueSeconds(1),   // minimum
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    private record RefreshBlockExpiration(Index index, long timestampInMillis) {}

    private final MasterServiceTaskQueue<RemoveRefreshBlockClusterStateUpdateTask> updateClusterStateTaskQueue;
    private final LinkedBlockingQueue<RefreshBlockExpiration> refreshBlocks = new LinkedBlockingQueue<>();
    private final AtomicInteger pendingRefreshBlocks = new AtomicInteger();
    private final ExecutorService executor;
    private final ThreadPool threadPool;

    private Scheduler.ScheduledCancellable scheduledExpirationCheck;
    private volatile TimeValue expireAfter;

    @SuppressWarnings("this-escape")
    public RemoveRefreshClusterBlockService(Settings settings, ClusterService clusterService, ThreadPool threadPool) {
        this.expireAfter = EXPIRE_AFTER_SETTING.get(settings);
        this.updateClusterStateTaskQueue = clusterService.createTaskQueue(
            "remove-refresh-blocks-cluster-state-update",
            Priority.HIGH,
            CLUSTER_STATE_EXECUTOR
        );
        this.threadPool = threadPool;
        this.executor = threadPool.executor(ThreadPool.Names.GENERIC);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(EXPIRE_AFTER_SETTING, this::setExpireAfter);
        clusterService.addListener(this);
    }

    public void setExpireAfter(TimeValue value) {
        logger.info("Updating refresh block keep-alive setting from [{}] to [{}]", expireAfter, value);
        this.expireAfter = value;
        scheduleExpirationCheck(delayed1sec(value.millis()));
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false || event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        @FixForMultiProject(description = "we may want to loop through all active projects")
        final var projectId = Metadata.DEFAULT_PROJECT_ID;
        var blockedIndices = getIndicesWithRefreshBlock(event.state(), projectId);
        if (blockedIndices.isEmpty()) {
            return;
        }

        boolean scheduleExpiration = false;
        if (event.nodesDelta().masterNodeChanged()) {
            logger.debug("Node is elected master, listing all indices with a refresh block");
            scheduleExpiration = addRefreshBlockExpirationEntry(event.state(), blockedIndices);

        } else if (event.metadataChanged()) {
            var previousBlockedIndices = getIndicesWithRefreshBlock(event.previousState(), projectId);

            var newBlockedIndices = Sets.difference(blockedIndices, previousBlockedIndices);
            if (newBlockedIndices.isEmpty() == false) {
                logger.debug("Found [{}] new indices with refresh block: {}", newBlockedIndices.size(), newBlockedIndices);
                assert newBlockedIndices.stream().allMatch(index -> event.previousState().metadata().getProject().hasIndex(index) == false);
                scheduleExpiration = addRefreshBlockExpirationEntry(event.state(), newBlockedIndices);
            }

            if (previousBlockedIndices.isEmpty() == false) {
                logger.debug("Found [{}] existing indices with refresh block, checking replicas", previousBlockedIndices.size());
                Set<Index> blocksToRemove = null;
                for (var previous : previousBlockedIndices) {
                    var indexMetadata = event.state().metadata().getProject().index(previous);
                    if (indexMetadata != null && indexMetadata.getNumberOfReplicas() == 0) {
                        logger.debug("Found index {} with refresh block but no replicas, removing block", indexMetadata.getIndex());
                        if (blocksToRemove == null) {
                            blocksToRemove = new HashSet<>();
                        }
                        blocksToRemove.add(indexMetadata.getIndex());
                    }
                }
                if (blocksToRemove != null && blocksToRemove.isEmpty() == false) {
                    updateClusterStateTaskQueue.submitTask(
                        "remove-refresh-blocks-no-replicas",
                        new RemoveRefreshBlockClusterStateUpdateTask(blocksToRemove),
                        null
                    );
                }
            }
        }

        if (event.routingTableChanged()) {
            Set<Index> blocksToRemove = new HashSet<>();
            for (String blockedIndex : blockedIndices) {
                if (event.indexRoutingTableChanged(event.state().metadata().getProject().index(blockedIndex).getIndex())) {
                    var indexRoutingTable = event.state().routingTable().index(blockedIndex);
                    if (indexRoutingTable.readyForSearch()) {
                        blocksToRemove.add(indexRoutingTable.getIndex());
                    }
                }
            }
            if (blocksToRemove.isEmpty() == false) {
                updateClusterStateTaskQueue.submitTask(
                    "remove-refresh-blocks-for-indices-ready-for-search",
                    new RemoveRefreshBlockClusterStateUpdateTask(Collections.unmodifiableSet(blocksToRemove)),
                    null
                );
            }
        }

        if (scheduleExpiration) {
            scheduleExpirationCheck(delayed1sec(expireAfter.millis()));
        }
    }

    private static Set<String> getIndicesWithRefreshBlock(ClusterState clusterState, ProjectId projectId) {
        return clusterState.blocks()
            .indices(projectId, ClusterBlockLevel.REFRESH)
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().isEmpty() == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    private boolean addRefreshBlockExpirationEntry(ClusterState clusterState, Set<String> indices) {
        final var timestampInMillis = threadPool.relativeTimeInMillis();

        boolean scheduleExpiration = false;
        for (var index : indices) {
            var indexMetadata = clusterState.metadata().getProject().index(index);
            assert clusterState.blocks().hasIndexBlock(index, IndexMetadata.INDEX_REFRESH_BLOCK) : index;

            refreshBlocks.add(new RefreshBlockExpiration(indexMetadata.getIndex(), timestampInMillis));
            if (pendingRefreshBlocks.incrementAndGet() == 1) {
                scheduleExpiration = true;
            }
        }
        return scheduleExpiration;
    }

    /**
     * Schedules the next expiration check to run after the provided {@code delay}. If a check is already scheduled, it is cancelled before
     * scheduling the new one.
     */
    private synchronized void scheduleExpirationCheck(TimeValue delay) {
        try {
            var scheduled = scheduledExpirationCheck;
            if (scheduled != null) {
                scheduled.cancel();
            }
            scheduledExpirationCheck = threadPool.schedule(new ExpirationCheck(), delay, executor);
        } catch (Exception e) {
            logException(e);
        }
    }

    /**
     * Check if the refresh blocks detected in cluster state are expired, and submit a cluster state update task to remove them if needed.
     *
     * @param timeInMillis          the current timestamp in milliseconds
     * @param expireAfterInMillis   the time in milliseconds after which a detected refresh block should be removed
     * @return a {@link ExpirationCheck.Result}
     */
    private synchronized ExpirationCheck.Result runExpirationCheck(final long timeInMillis, final long expireAfterInMillis) {
        final var blocksToRemove = new HashSet<Index>();

        long nextBlockTimestampInMillis = 0L;
        RefreshBlockExpiration block;
        while ((block = refreshBlocks.peek()) != null) {
            if (timeInMillis < block.timestampInMillis() + expireAfterInMillis) {
                logger.trace("No more expired refresh block");
                nextBlockTimestampInMillis = block.timestampInMillis();
                break;
            }
            logger.trace("{} Found expired refresh block", block.index());
            var removed = refreshBlocks.poll();
            blocksToRemove.add(block.index());
            assert removed == block;
        }

        if (blocksToRemove.isEmpty() == false) {
            updateClusterStateTaskQueue.submitTask(
                "remove-expired-refresh-blocks",
                new RemoveRefreshBlockClusterStateUpdateTask(Set.copyOf(blocksToRemove)),
                null
            );
            return new ExpirationCheck.Result(blocksToRemove.size(), nextBlockTimestampInMillis);
        }
        return new ExpirationCheck.Result(0, nextBlockTimestampInMillis);
    }

    /**
     * Runnable to execute an expiration check. Reschedules a new check if needed.
     */
    private class ExpirationCheck extends AbstractRunnable {

        record Result(int removedBlocks, long nextBlockTimestampInMillis) {}

        private Result result;

        @Override
        protected void doRun() {
            result = runExpirationCheck(threadPool.relativeTimeInMillis(), expireAfter.millis());
        }

        @Override
        public void onAfter() {
            if (result != null) {
                var pendings = pendingRefreshBlocks.addAndGet(-result.removedBlocks);
                assert 0 <= pendings : pendings;
                if (0 < pendings) {
                    long nextDelayInMillis = expireAfter.millis();
                    if (result.nextBlockTimestampInMillis() > 0L) {
                        nextDelayInMillis = Math.max(
                            result.nextBlockTimestampInMillis() + nextDelayInMillis - threadPool.relativeTimeInMillis(),
                            0L
                        );
                    }
                    scheduleExpirationCheck(delayed1sec(nextDelayInMillis));
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            logException(e);
        }
    }

    // package-private for tests
    Set<Index> blockedIndices() {
        return refreshBlocks.stream().map(RefreshBlockExpiration::index).collect(Collectors.toUnmodifiableSet());
    }

    private static void logException(Exception e) {
        if (e instanceof EsRejectedExecutionException rejectedException) {
            if (rejectedException.isExecutorShutdown()) {
                logger.debug("Failed to schedule next removal of refresh blocks, node is shutting down", e);
                return;
            }
        }
        logger.warn("Failed to remove refresh blocks", e);
    }

    private static TimeValue delayed1sec(long millis) {
        return TimeValue.timeValueMillis(millis + 1_000L);
    }

    /**
     * A cluster state update task that removes the {{@link IndexMetadata#INDEX_REFRESH_BLOCK}} for a set of indices.
     */
    private record RemoveRefreshBlockClusterStateUpdateTask(Set<Index> indices) implements ClusterStateTaskListener {

        private RemoveRefreshBlockClusterStateUpdateTask {
            assert indices != null;
            assert indices.isEmpty() == false;
        }

        private ClusterState execute(ClusterState currentState) {
            ClusterBlocks.Builder updatedBlocks = null;
            for (var index : indices) {
                if (currentState.blocks().hasIndexBlock(index.getName(), IndexMetadata.INDEX_REFRESH_BLOCK) == false) {
                    continue;
                }
                if (updatedBlocks == null) {
                    updatedBlocks = ClusterBlocks.builder(currentState.blocks());
                }
                logger.trace("{} Removing expired refresh block from cluster state", index);
                @FixForMultiProject
                final var projectId = Metadata.DEFAULT_PROJECT_ID;
                updatedBlocks.removeIndexBlock(projectId, index.getName(), IndexMetadata.INDEX_REFRESH_BLOCK);
            }
            if (updatedBlocks != null) {
                return ClusterState.builder(currentState).blocks(updatedBlocks).build();
            }
            return currentState;
        }

        @Override
        public void onFailure(Exception e) {
            logger.debug(() -> "Failed to remove refresh block for indices: " + indices, e);
        }
    }

    private static final SimpleBatchedExecutor<RemoveRefreshBlockClusterStateUpdateTask, Void> CLUSTER_STATE_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Void> executeTask(RemoveRefreshBlockClusterStateUpdateTask task, ClusterState clusterState) {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(RemoveRefreshBlockClusterStateUpdateTask task, Void unused) {
                logger.debug("Refresh blocks removed successfully for indices: {}", task.indices);
            }
        };
}
