/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.repositories.ProjectRepo.projectRepoString;

/**
 * Looks after batching-up the start of a collection of snapshot deletions in a single repository.
 * <p>
 * Cannot just use a {@link MasterServiceTaskQueue} directly because it needs the latest {@link RepositoryData} to compute the cluster state
 * update, so this value must be included in the {@link ClusterStateTaskExecutor.BatchExecutionContext}, and the tasks must all be retried
 * if the {@link RepositoryData} changes before the batch executes.
 */
final class SnapshotDeletionStartBatcher {

    private static final Logger logger = LogManager.getLogger(SnapshotDeletionStartBatcher.class);

    private final RepositoriesService repositoriesService;
    private final ThreadPool threadPool;
    private final ProjectId projectId;
    private final String repositoryName;
    private final Consumer<Snapshot> notifyAbortedByDeletion;
    private final SnapshotEnder snapshotEnder;
    private final ItemCompletionHandler subscribeToPendingDelete;
    private final DeletionStarter deletionStarter;
    private final String queueName;
    private final MasterServiceTaskQueue<Batch> snapshotDeletionBatchTaskQueue;
    private final ExecutorService snapshotExecutor;

    /**
     * @param repositoriesService The {@link RepositoriesService}, needed because the underlying {@link Repository} instance may change from
     *                            time to time as settings are updated etc.
     * @param clusterService The {@link ClusterService} used to construct the task queue.
     * @param threadPool     The {@link ThreadPool}, used to supply a {@link ThreadContext}, to schedule timeouts, and as a time source.
     * @param projectId      The {@link ProjectId} of the associated repository.
     * @param repositoryName The name of the associated repository.
     * @param notifyAbortedByDeletion Callback invoked after successfully updating the cluster state where the cluster state update has
     *                                aborted a running snapshot before that snapshot wrote any data to the repository (i.e. while all
     *                                shards were in state {@link SnapshotsInProgress.ShardState#QUEUED}). Such snapshots need no actual
     *                                deletion to occur, and therefore need not even be finalized: they are simply removed from the cluster
     *                                state, and any listeners waiting for the completion of the snapshot are immediately notified of the
     *                                failure.
     * @param snapshotEnder Callback invoked after successfully updating the cluster state where the cluster state update has aborted a
     *                      running snapshot after that snapshot has written some data to the repository, and the abort itself has made the
     *                      snapshot ready to finalize (i.e. all remaining incomplete shards moved straight from
     *                      {@link SnapshotsInProgress.ShardState#QUEUED} to {@link SnapshotsInProgress.ShardState#FAILED} leaving none in
     *                      state {@link SnapshotsInProgress.ShardState#ABORTED}).
     * @param subscribeToPendingDelete Callback invoked after successfully updating the cluster state to reflect the requested aborts, to
     *                                 subscribe all the listeners submitted to {@link #startDeletion} to wait for a particular
     *                                 {@link SnapshotDeletionsInProgress.Entry} to complete.
     * @param deletionStarter Callback invoked after successfully updating the cluster state to add a new
     *                        {@link SnapshotDeletionsInProgress.Entry} which is already in state
     *                        {@link SnapshotDeletionsInProgress.State#STARTED}, in order to trigger the execution of this deletion.
     */
    SnapshotDeletionStartBatcher(
        RepositoriesService repositoriesService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ProjectId projectId,
        String repositoryName,
        Consumer<Snapshot> notifyAbortedByDeletion,
        SnapshotEnder snapshotEnder,
        ItemCompletionHandler subscribeToPendingDelete,
        DeletionStarter deletionStarter
    ) {
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
        this.projectId = projectId;
        this.repositoryName = repositoryName;
        this.notifyAbortedByDeletion = notifyAbortedByDeletion;
        this.snapshotEnder = snapshotEnder;
        this.subscribeToPendingDelete = subscribeToPendingDelete;
        this.deletionStarter = deletionStarter;
        this.queueName = "snapshot-deletion-start" + ProjectRepo.projectRepoString(projectId, repositoryName);
        this.snapshotDeletionBatchTaskQueue = clusterService.createTaskQueue(queueName, Priority.NORMAL, new Executor());
        this.snapshotExecutor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
    }

    /**
     * A single snapshot deletion request, to be added to the cluster state's {@link SnapshotDeletionsInProgress}.
     */
    private static final class SnapshotDeletionsItem {

        /**
         * Snapshot names and patterns from the original request.
         */
        final String[] snapshots;

        /**
         * Whether to notify the listener after updating the cluster state to start the deletion, or to wait for the deletion to complete.
         */
        final boolean waitForCompletion;

        /**
         * One ref if the timeout has not yet expired, and one ref if the task is being processed and has got past the point of a possible
         * timeout.
         */
        final RefCounted timeoutRefs;

        /**
         * Listener to be completed by success or timeout or other failure.
         */
        final SubscribableListener<Void> listener;

        /**
         * Callback to run when the task is processed; set during execution of the batch of tasks
         */
        @Nullable // if the item has not been resolved yet
        ItemCompletionHandler itemCompletionHandler;

        /**
         * If the item targets only snapshots that are already targetted by a STARTED deletion on this repository, it should be subscribed
         * to this UUID rather than the one that the batch creates.
         */
        @Nullable
        String startedDeletionUuid;

        SnapshotDeletionsItem(String[] snapshots, boolean waitForCompletion, RefCounted timeoutRefs, SubscribableListener<Void> listener) {
            this.snapshots = snapshots;
            this.waitForCompletion = waitForCompletion;
            this.timeoutRefs = timeoutRefs;
            this.listener = listener;
        }
    }

    /**
     * Queue of deletions waiting to be added to the cluster state.
     */
    private final ArrayDeque<SnapshotDeletionsItem> snapshotDeletionsItems = new ArrayDeque<>();

    /**
     * Number of entries in {@link #snapshotDeletionsItems} ready to be processed. Includes timed-out entries and entries waiting
     * to retry.
     */
    private final AtomicInteger snapshotDeletionForBatchingCount = new AtomicInteger();

    /**
     * Adjust the cluster state to (effectively) enqueue some snapshot deletions and then (if possible) process them.
     * <p>
     * "Effectively" because deletion of a not-started-yet snapshot just drops it from the {@link SnapshotsInProgress} straight away.
     * We only create a {@link SnapshotDeletionsInProgress} entry if there's snapshots that need to be cleaned up.
     * <p>
     * "If possible" because deletions only run when there's no snapshots being taken - otherwise, the {@link SnapshotDeletionsInProgress}
     * entry is left in state {@link SnapshotDeletionsInProgress.State#WAITING} and will be picked up when a snapshot finalization
     * removes the last entry from {@link SnapshotsInProgress}.
     */
    void startDeletion(String[] snapshots, boolean waitForCompletion, TimeValue timeout, ActionListener<Void> listener) {
        logger.trace("startDeletion[{}]", Arrays.toString(snapshots));
        final RefCounted timeoutRefs;
        final SubscribableListener<Void> wrappedListener = new SubscribableListener<>();
        wrappedListener.addListener(listener, EsExecutors.DIRECT_EXECUTOR_SERVICE, threadPool.getThreadContext());
        if (MasterService.isInfiniteTaskTimeout(timeout)) {
            timeoutRefs = RefCounted.ALWAYS_REFERENCED;
        } else {
            timeoutRefs = AbstractRefCounted.of(
                () -> wrappedListener.onFailure(
                    new ElasticsearchTimeoutException(queueName + " did not start snapshot deletion within [" + timeout + "]")
                )
            );
            final var cancellable = threadPool.schedule(timeoutRefs::decRef, timeout, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            wrappedListener.addListener(ActionListener.running(cancellable::cancel));
        }

        synchronized (snapshotDeletionsItems) {
            snapshotDeletionsItems.addLast(new SnapshotDeletionsItem(snapshots, waitForCompletion, timeoutRefs, wrappedListener));
        }
        if (snapshotDeletionForBatchingCount.getAndIncrement() == 0) {
            runQueueProcessor();
        }
    }

    private void runQueueProcessor() {
        final var batch = new SnapshotDeletionsItem[snapshotDeletionForBatchingCount.get()];
        logger.trace("runQueueProcessor: processing batch of size [{}]", batch.length);
        assert batch.length > 0; // implies that this is single-threaded
        synchronized (snapshotDeletionsItems) {
            int i = 0;
            for (var snapshotDeletionForBatching : snapshotDeletionsItems) {
                // do not pull items from queue yet, we may need to retry if the RepositoryData changes
                batch[i++] = snapshotDeletionForBatching;
                if (i == batch.length) {
                    break;
                }
            }
            assert i == batch.length;
        }

        final Repository repository;
        final RepositoryMetadata repositoryMetadata;

        try {
            repository = repositoriesService.repository(projectId, repositoryName);
            repositoryMetadata = repository.getMetadata();
        } catch (Exception e) {
            failBatch(batch, e);
            return;
        }

        repository.getRepositoryData(snapshotExecutor, new ActionListener<>() {
            @Override
            public void onResponse(RepositoryData repositoryData) {
                snapshotDeletionBatchTaskQueue.submitTask(
                    queueName + "[" + batch.length + "]",
                    new Batch(batch, repositoryMetadata, repositoryData),
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT
                );
            }

            @Override
            public void onFailure(Exception e) {
                failBatch(batch, e);
            }
        });
    }

    private void completeBatch(int batchSize) {
        synchronized (snapshotDeletionsItems) {
            for (int i = 0; i < batchSize; i++) {
                final var item = snapshotDeletionsItems.pollFirst();
                assert item != null;
                assert item.timeoutRefs.hasReferences() == false || item.listener.isDone() || item.waitForCompletion;
            }
        }
        if (snapshotDeletionForBatchingCount.addAndGet(-batchSize) > 0) {
            snapshotExecutor.execute(this::runQueueProcessor);
        }
    }

    private void failBatch(SnapshotDeletionsItem[] batch, Exception e) {
        synchronized (snapshotDeletionsItems) {
            for (var item : batch) {
                final var fromQueue = snapshotDeletionsItems.removeFirst();
                assert fromQueue == item;
            }
        }

        for (var item : batch) {
            item.listener.onFailure(e); // NB outside mutex
        }

        if (snapshotDeletionForBatchingCount.addAndGet(-batch.length) > 0) {
            snapshotExecutor.execute(this::runQueueProcessor);
        }
    }

    /**
     * Resolve the items in the batch into concrete {@link SnapshotId} instances, and also configure each
     * {@link SnapshotDeletionsItem#itemCompletionHandler} and {@link SnapshotDeletionsItem#startedDeletionUuid} according to the conditions
     * on which the item eventually needs to wait.
     */
    private Set<SnapshotId> resolveSnapshotIdsAndItemCompletionHandlers(
        SnapshotDeletionsItem[] batch,
        ClusterState initialState,
        List<SnapshotsInProgress.Entry> repoSnapshotsInProgress,
        RepositoryData repositoryData,
        BatchCompletionHandler batchCompletionHandler
    ) {
        final HashSet<SnapshotId> activeRestoreSources = new HashSet<>();
        final RestoreInProgress restoreInProgress = RestoreInProgress.get(initialState);
        for (final var entry : restoreInProgress) {
            if (repositoryName.equals(entry.snapshot().getRepository()) && projectId.equals(entry.snapshot().getProjectId())) {
                activeRestoreSources.add(entry.snapshot().getSnapshotId());
            }
        }

        final HashSet<SnapshotId> activeCloneSources = new HashSet<>();
        for (final var entry : repoSnapshotsInProgress) {
            if (entry.isClone()) {
                activeCloneSources.add(entry.source());
            }
        }

        final var completedSnapshotCount = repositoryData.getSnapshotIds().size();
        final Map<String, SnapshotId> snapshotsIdsInRepository = Maps.newHashMapWithExpectedSize(completedSnapshotCount);
        for (var snapshotId : repositoryData.getSnapshotIds()) {
            snapshotsIdsInRepository.put(snapshotId.getName(), snapshotId);
        }

        final Set<SnapshotId> snapshotIds = new HashSet<>();

        class ItemCompletionHandlerResolver {

            // working space for each item: cleared each time but kept in place to save on allocations
            private final Set<SnapshotId> itemCompletedSnapshotIds = Sets.newHashSetWithExpectedSize(completedSnapshotCount);
            private final Set<SnapshotId> itemInProgressSnapshotIds = Sets.newHashSetWithExpectedSize(repoSnapshotsInProgress.size());
            private final Set<String> itemUnmatchedNames = new HashSet<>();

            private final Set<SnapshotId> startedDeletionSnapshots = new HashSet<>();
            private final String startedDeletionUuid = resolveStartedDeletionSnapshots(initialState, startedDeletionSnapshots);

            ItemCompletionHandler resolveItemAndAddSnapshotIds(SnapshotDeletionsItem item) {
                try {
                    assert item.itemCompletionHandler == null;
                    assert item.startedDeletionUuid == null;

                    assert itemCompletedSnapshotIds.isEmpty();
                    assert itemInProgressSnapshotIds.isEmpty();
                    assert itemUnmatchedNames.isEmpty();

                    if (item.timeoutRefs.tryIncRef() == false) {
                        // timeout elapsed, nothing to do here
                        return ItemCompletionHandler.DO_NOTHING;
                    }
                    assert item.listener.isDone() == false : Arrays.toString(item.snapshots);

                    resolveCompletedSnapshots(item);
                    resolveInProgressSnapshots(item);

                    // ensure all non-wildcard names are matched
                    if (itemUnmatchedNames.isEmpty() == false) {
                        return failItem(new SnapshotMissingException(repositoryName, itemUnmatchedNames.iterator().next()));
                    }

                    // skip no-op items
                    if (itemCompletedSnapshotIds.isEmpty() && itemInProgressSnapshotIds.isEmpty()) {
                        return ItemCompletionHandler.COMPLETE_LISTENER_IMMEDIATELY;
                    }

                    // check for ongoing operations which conflict
                    final var concurrentSnapshotExecutionException = checkConcurrentClonesAndRestores();
                    if (concurrentSnapshotExecutionException != null) {
                        return failItem(concurrentSnapshotExecutionException);
                    }

                    if (startedDeletionSnapshots.containsAll(itemCompletedSnapshotIds)
                        && startedDeletionSnapshots.containsAll(itemInProgressSnapshotIds)) {
                        // item is only targeting snapshots whose deletion is already present and STARTED so it need only wait for that
                        // deletion to finish, even if the rest of the batch creates/updates a WAITING deletion
                        batchCompletionHandler.appendToFinalLog(item.snapshots);
                        if (item.waitForCompletion) {
                            item.startedDeletionUuid = startedDeletionUuid;
                            return subscribeToPendingDelete;
                        } else {
                            return ItemCompletionHandler.COMPLETE_LISTENER_IMMEDIATELY;
                        }
                    }
                    // else add all the item's snapshots to a WAITING deletion D2 (creating D2 if it does not already exist) even though
                    // this may duplicate entries which are already present in an ongoing STARTED deletion D1: D1 may fail but if D2 then
                    // goes on to succeed then it's important that D2 alone really did delete all the snapshots requested by this item,
                    // because the item's listener will report only the outcome of D2.

                    snapshotIds.addAll(itemCompletedSnapshotIds);
                    snapshotIds.addAll(itemInProgressSnapshotIds);
                    batchCompletionHandler.appendToFinalLog(item.snapshots);
                    return item.waitForCompletion ? subscribeToPendingDelete : ItemCompletionHandler.COMPLETE_LISTENER_IMMEDIATELY;
                } finally {
                    itemCompletedSnapshotIds.clear();
                    itemInProgressSnapshotIds.clear();
                    itemUnmatchedNames.clear();
                }
            }

            private void resolveCompletedSnapshots(SnapshotDeletionsItem item) {
                for (String snapshotOrPattern : item.snapshots) {
                    if (Regex.isSimpleMatchPattern(snapshotOrPattern)) {
                        for (final var snapshotId : repositoryData.getSnapshotIds()) {
                            if (Regex.simpleMatch(snapshotOrPattern, snapshotId.getName())) {
                                itemCompletedSnapshotIds.add(snapshotId);
                            }
                        }
                    } else {
                        final SnapshotId foundId = snapshotsIdsInRepository.get(snapshotOrPattern);
                        if (foundId == null) {
                            itemUnmatchedNames.add(snapshotOrPattern);
                        } else {
                            itemCompletedSnapshotIds.add(foundId);
                        }
                    }
                }
            }

            private void resolveInProgressSnapshots(SnapshotDeletionsItem item) {
                for (SnapshotsInProgress.Entry entry : repoSnapshotsInProgress) {
                    final SnapshotId snapshotId = entry.snapshot().getSnapshotId();
                    if (Regex.simpleMatch(item.snapshots, snapshotId.getName())) {
                        itemInProgressSnapshotIds.add(snapshotId);
                        itemUnmatchedNames.remove(snapshotId.getName());
                    }
                }
            }

            private ConcurrentSnapshotExecutionException checkConcurrentClonesAndRestores() {
                for (final var snapshotId : itemCompletedSnapshotIds) {
                    if (activeCloneSources.contains(snapshotId)) {
                        return new ConcurrentSnapshotExecutionException(
                            new Snapshot(projectId, repositoryName, snapshotId),
                            "cannot delete snapshot while it is being cloned"
                        );
                    }
                    if (activeRestoreSources.contains(snapshotId)) {
                        return new ConcurrentSnapshotExecutionException(
                            new Snapshot(projectId, repositoryName, snapshotId),
                            "cannot delete snapshot during a restore in progress in [" + restoreInProgress + "]"
                        );
                    }
                }
                return null;
            }

            private ItemCompletionHandler failItem(Exception exception) {
                return (ignoredUuid, listener) -> listener.onFailure(exception);
            }

            @Nullable // if no STARTED snapshot deletion running
            private static String resolveStartedDeletionSnapshots(ClusterState clusterState, Set<SnapshotId> startedDeletionSnapshots) {
                assert startedDeletionSnapshots.isEmpty(); // only one STARTED deletion at once
                for (var snapshotDeletionInProgress : SnapshotDeletionsInProgress.get(clusterState).getEntries()) {
                    if (snapshotDeletionInProgress.state() == SnapshotDeletionsInProgress.State.STARTED) {
                        startedDeletionSnapshots.addAll(snapshotDeletionInProgress.snapshots());
                        return snapshotDeletionInProgress.uuid();
                    }
                }
                return null;
            }
        }

        final var itemCompletionHandlerResolver = new ItemCompletionHandlerResolver();
        for (final var item : batch) {
            item.itemCompletionHandler = itemCompletionHandlerResolver.resolveItemAndAddSnapshotIds(item);
        }
        return snapshotIds;
    }

    /**
     * A cluster-state update task for a batch of snapshot deletions. Executed by {@link Executor}.
     */
    private final class Batch implements ClusterStateTaskListener {
        final SnapshotDeletionsItem[] batch;
        final RepositoryMetadata repositoryMetadata;
        final RepositoryData repositoryData;
        // TODO could we discard the repositoryData eagerly if repositoryMetadata changes to release that memory sooner?

        Batch(SnapshotDeletionsItem[] batch, RepositoryMetadata repositoryMetadata, RepositoryData repositoryData) {
            this.batch = batch;
            this.repositoryMetadata = repositoryMetadata;
            this.repositoryData = repositoryData;
        }

        @Override
        public void onFailure(Exception e) {
            failBatch(batch, e);
        }
    }

    /**
     * Computes the cluster state update that adds a batch of deletions to the queue and possibly aborts some of the running snapshots.
     */
    private class Executor implements ClusterStateTaskExecutor<Batch> {
        @Override
        public ClusterState execute(BatchExecutionContext<Batch> batchExecutionContext) {
            final var initialState = batchExecutionContext.initialState();

            final var projectMetadata = initialState.metadata().getProject(projectId);
            SnapshotsServiceUtils.ensureRepositoryExists(repositoryName, projectMetadata);

            assert batchExecutionContext.taskContexts().size() == 1;
            final var taskContext = batchExecutionContext.taskContexts().getFirst();
            final var task = taskContext.getTask();

            final var repositoryMetadata = RepositoriesMetadata.get(projectMetadata).repository(repositoryName);
            // Similar to SnapshotsService#executeConsistentStateUpdate: check RepositoryMetadata equality.
            if (task.repositoryMetadata.equals(repositoryMetadata) == false) {
                // RepositoryMetadata changed out from under us, so the RepositoryData we captured may be stale. Retry from start.
                // NB this retry-from-start is copied from the old unbatched mechanism but seems a little drastic. It would be neater to
                // allow updating of task.repositoryData while the task is in the queue, ideally by subscribing to repositoryData updates
                // from the repository itself, but that kind of subscription mechanism does not exist today. If we had that, we could just
                // use a regular master task queue to do this batching work for us.
                logger.trace("repo metadata changed: expected {} but got {}, retrying", task.repositoryMetadata, repositoryMetadata);
                taskContext.success(SnapshotDeletionStartBatcher.this::runQueueProcessor);
                return initialState;
            }

            // NB it is ok if we're in the middle of a RepositoryData update (i.e. the pending and safe generations are unequal) because
            // this update will either be finalizing the creation of a snapshot or the deletion of some collection of snapshots. If this
            // batch requests to delete a snapshot whose creation that is being finalized then this snapshot will be added to the deletion
            // queue, requiring the finalization to complete before the deletion starts. On the other hand if it requests to delete a
            // snapshot whose deletion is already being finalized then the listener will be subscribed to that deletion and need not be
            // added to the cluster state (see SnapshotDeletionsItem#startedDeletionUuid).
            // TODO does this mean that here it's enough to check the safe generation?

            final var batchCompletionHandler = new BatchCompletionHandler(initialState, task.repositoryData, task.batch);
            taskContext.success(batchCompletionHandler);

            final var snapshotsInProgress = SnapshotsInProgress.get(initialState);
            final var repoSnapshotsInProgress = snapshotsInProgress.forRepo(projectId, repositoryName);

            final var snapshotIds = resolveSnapshotIdsAndItemCompletionHandlers(
                task.batch,
                initialState,
                repoSnapshotsInProgress,
                task.repositoryData,
                batchCompletionHandler
            );
            if (snapshotIds.isEmpty()) {
                logger.debug("snapshot deletion targets no snapshots");
                return initialState;
            }

            // Check these _after_ checking to make sure the request is not a no-op because no-op deletes are ok in these cases:
            SnapshotsServiceUtils.ensureNotReadOnly(projectMetadata, repositoryName);
            SnapshotsServiceUtils.ensureNoCleanupInProgress(initialState, repositoryName, "<delete>", "delete snapshot");

            // Snapshot ids that will have to be physically deleted from the repository - either completed snapshots or running snapshots
            // that have started to write to the repository and therefore need to be allowed to finalize first. Note that this will include
            // snapshots whose deletion is already in progress (either WAITING or STARTED).
            final Set<SnapshotId> snapshotIdsRequiringCleanup = new HashSet<>(snapshotIds);
            final var updatedSnapshots = abortRunningSnapshots(
                snapshotsInProgress,
                repoSnapshotsInProgress,
                snapshotIdsRequiringCleanup,
                batchCompletionHandler
            );

            if (snapshotIdsRequiringCleanup.isEmpty()) {
                // We only saw snapshots that could be removed from the cluster state right away, no need to update the deletions
                logger.debug("snapshot deletion targets no completed snapshots");
                return SnapshotsServiceUtils.updateWithSnapshots(initialState, updatedSnapshots, null);
            }

            final SnapshotDeletionsInProgress deletionsInProgress = SnapshotDeletionsInProgress.get(initialState);

            // Check for an existing STARTED deletion that's already going to delete all the target snapshots: in this case no cluster state
            // update is needed.
            for (var entry : deletionsInProgress.getEntries()) {
                if (entry.projectId().equals(projectId)
                    && entry.repository().equals(repositoryName)
                    && entry.state() == SnapshotDeletionsInProgress.State.STARTED) {
                    final var runningSnapshotIds = Set.copyOf(entry.snapshots());
                    if (runningSnapshotIds.containsAll(snapshotIds)) {
                        // NB copied over from the unbatched implementation, may now already be handled in resolveItemAndAddSnapshotIds?
                        assert false : "should be already handled"; // TODO remove all this if tests are reliably passing

                        // all target snapshots are already marked for deletion so any running ones must already be aborted
                        assert updatedSnapshots == snapshotsInProgress;
                        batchCompletionHandler.deletionUuid = entry.uuid();
                        logger.debug("new snapshot deletions are a subset of running deletions, nothing to do");
                        return initialState;
                    }
                }
            }

            // Check for an existing WAITING deletion for the right repository: in this case we can combine it with the new snapshots.
            for (var entry : deletionsInProgress.getEntries()) {
                if (entry.projectId().equals(projectId)
                    && entry.repository().equals(repositoryName)
                    && entry.state() == SnapshotDeletionsInProgress.State.WAITING) {

                    batchCompletionHandler.deletionUuid = entry.uuid();
                    logger.debug("combining new snapshot deletions with existing waiting batch");
                    final var updatedDeletionsInProgress = deletionsInProgress.withReplacedEntry(
                        entry.withAddedSnapshots(snapshotIdsRequiringCleanup)
                    );
                    if (updatedSnapshots == snapshotsInProgress && updatedDeletionsInProgress == deletionsInProgress) {
                        return initialState;
                    } else {
                        return SnapshotsServiceUtils.updateWithSnapshots(initialState, updatedSnapshots, updatedDeletionsInProgress);
                    }
                }
            }

            // Otherwise we must add a new snapshot deletion to the cluster state
            final var newEntryState = updatedSnapshots.forRepo(projectId, repositoryName)
                .stream()
                .noneMatch(SnapshotsServiceUtils::isWritingToRepository)
                && deletionsInProgress.hasExecutingDeletion(projectId, repositoryName) == false
                    ? SnapshotDeletionsInProgress.State.STARTED
                    : SnapshotDeletionsInProgress.State.WAITING;
            final var newEntry = new SnapshotDeletionsInProgress.Entry(
                projectId,
                repositoryName,
                List.copyOf(snapshotIdsRequiringCleanup),
                threadPool.absoluteTimeInMillis(),
                task.repositoryData.getGenId(),
                newEntryState
            );
            batchCompletionHandler.deletionUuid = newEntry.uuid();

            if (newEntryState == SnapshotDeletionsInProgress.State.STARTED) {
                logger.debug("creating new batch of deletions and starting their processing");
                batchCompletionHandler.entryToStart = newEntry;
            } else {
                logger.debug("creating new batch of deletions to be executed later");
            }

            return SnapshotsServiceUtils.updateWithSnapshots(initialState, updatedSnapshots, deletionsInProgress.withAddedEntry(newEntry));
        }

        /**
         * Compute an updated {@link SnapshotsInProgress} marking any affected snapshots as {@link SnapshotsInProgress.State#ABORTED}.
         */
        private SnapshotsInProgress abortRunningSnapshots(
            SnapshotsInProgress snapshotsInProgress,
            List<SnapshotsInProgress.Entry> repoSnapshotsInProgress,
            Set<SnapshotId> snapshotIdsRequiringCleanup,
            BatchCompletionHandler batchCompletionHandler
        ) {
            final List<SnapshotsInProgress.Entry> updatedEntries = new ArrayList<>(repoSnapshotsInProgress.size());
            boolean anySnapshotsAborted = false;
            for (var existing : repoSnapshotsInProgress) {
                if (existing.state() == SnapshotsInProgress.State.STARTED
                    && snapshotIdsRequiringCleanup.contains(existing.snapshot().getSnapshotId())) {
                    // snapshot is started - mark every non-completed shard as aborted
                    anySnapshotsAborted = true;
                    final SnapshotsInProgress.Entry abortedEntry = existing.abort();
                    if (abortedEntry == null) {
                        // No work has been done for this snapshot yet so we remove it from the cluster state directly
                        final Snapshot existingNotYetStartedSnapshot = existing.snapshot();
                        batchCompletionHandler.abortedAndImmediatelyRemoved.add(existingNotYetStartedSnapshot);
                        snapshotIdsRequiringCleanup.remove(existingNotYetStartedSnapshot.getSnapshotId());
                    } else {
                        if (abortedEntry.state().completed()) {
                            batchCompletionHandler.abortedAndNeedingFinalization.add(abortedEntry);
                        }
                        updatedEntries.add(abortedEntry);
                    }
                } else {
                    updatedEntries.add(existing);
                }
            }
            return anySnapshotsAborted
                ? snapshotsInProgress.createCopyWithUpdatedEntriesForRepo(projectId, repositoryName, updatedEntries)
                : snapshotsInProgress;
        }

        @Override
        public String describeTasks(List<Batch> tasks) {
            final var output = new StringBuilder();
            final var collector = new Strings.BoundedDelimitedStringCollector(output, ", ", 1024);
            for (var task : tasks) {
                for (var item : task.batch) {
                    for (var snapshot : item.snapshots) {
                        collector.appendItem("[" + snapshot + "]");
                    }
                }
            }
            collector.finish();
            return output.toString();
        }
    }

    /**
     * Collects up the tasks (completing listeners and triggering follow-ups) to execute when/if the add-deletions update succeeds.
     */
    private class BatchCompletionHandler implements Runnable {

        private final Metadata metadata;

        private final RepositoryData repositoryData;

        private final SnapshotDeletionsItem[] batch;

        private final IndexVersion maxDataNodeCompatibleIndexVersion;

        // Snapshots to be aborted that had all of their shard snapshots in QUEUED state, hence have not yet written anything to the
        // repository, and thus they could be removed from the cluster state right away, but we now need to notify their listeners.
        final Collection<Snapshot> abortedAndImmediatelyRemoved = new ArrayList<>();

        // Snapshots to be aborted that already potentially wrote something to the repository, but became complete due to the abort
        // (e.g. all shard snapshots were either complete or QUEUED, since the QUEUED ones all move straight to FAILED) so their
        // finalizations must now be started.
        final Collection<SnapshotsInProgress.Entry> abortedAndNeedingFinalization = new ArrayList<>();

        // Constructs the log message
        private final StringBuilder finalLogBuilder = new StringBuilder("deleting snapshots [");
        private boolean finalLogBuilderStarted;
        private final Strings.BoundedDelimitedStringCollector finalLogCollector = new Strings.BoundedDelimitedStringCollector(
            finalLogBuilder,
            ", ",
            ByteSizeUnit.KB.toIntBytes(4)
        );

        // A deletion that's ready to start processing straight away.
        @Nullable
        SnapshotDeletionsInProgress.Entry entryToStart;

        /**
         * Identifies the deletion task in the cluster state, so that wait_for_completion tasks can subscribe to it.
         */
        @Nullable
        String deletionUuid;

        BatchCompletionHandler(ClusterState clusterState, RepositoryData repositoryData, SnapshotDeletionsItem[] batch) {
            this.metadata = clusterState.metadata();
            this.maxDataNodeCompatibleIndexVersion = clusterState.nodes().getMaxDataNodeCompatibleIndexVersion();
            this.repositoryData = repositoryData;
            this.batch = batch;
        }

        @Override
        public void run() {
            if (finalLogBuilderStarted) {
                finalLogCollector.finish();
                finalLogBuilder.append("] from repository ").append(projectRepoString(projectId, repositoryName));
                logger.info(finalLogBuilder::toString);
            }

            for (var snapshot : abortedAndImmediatelyRemoved) {
                notifyAbortedByDeletion.accept(snapshot);
            }

            assert abortedAndNeedingFinalization.isEmpty() || entryToStart == null
                : "unexpectedly completed " + abortedAndNeedingFinalization + " while starting " + entryToStart;

            for (var entry : abortedAndNeedingFinalization) {
                snapshotEnder.endSnapshot(entry, metadata, repositoryData);
            }

            if (entryToStart != null) {
                deletionStarter.startDeletion(projectId, repositoryName, entryToStart, repositoryData, maxDataNodeCompatibleIndexVersion);
            }

            for (var item : batch) {
                assert item.itemCompletionHandler != null;
                item.itemCompletionHandler.onCompletion(
                    item.startedDeletionUuid != null ? item.startedDeletionUuid : deletionUuid,
                    item.listener
                );
            }

            completeBatch(batch.length);
        }

        void appendToFinalLog(String[] snapshots) {
            if (snapshots.length == 0) {
                return;
            }

            finalLogBuilderStarted = true;
            for (var snapshot : snapshots) {
                finalLogCollector.appendItem(snapshot);
            }
        }
    }

    public interface SnapshotEnder {
        /**
         * Trigger the finalization of a newly-ready-to-finalize snapshot.
         */
        void endSnapshot(SnapshotsInProgress.Entry entry, Metadata metadata, RepositoryData repositoryData);
    }

    public interface DeletionStarter {
        /**
         * Trigger the start of a newly-ready-to-process deletion.
         */
        void startDeletion(
            ProjectId projectId,
            String repositoryName,
            SnapshotDeletionsInProgress.Entry deleteEntry,
            RepositoryData repositoryData,
            IndexVersion minNodeVersion
        );
    }

    /**
     * Completion handler for individual items in the batch - e.g. completing the item's listener straight away, or subscribing to an
     * enqueued delete operation by UUID.
     */
    interface ItemCompletionHandler {

        /**
         * Called when the item has been processed (e.g. enqueued in the cluster state).
         * @param deletionUuid UUID of the {@link SnapshotDeletionsInProgress.Entry}, if applicable
         * @param itemListener Listener for the item that was processed.
         */
        void onCompletion(@Nullable String deletionUuid, ActionListener<Void> itemListener);

        /**
         * Completion handler for already-complete (e.g. timed-out) tasks.
         */
        ItemCompletionHandler DO_NOTHING = (ignoredUuid, ignoredListener) -> {};

        /**
         * Completion handler for tasks that do not need to wait for the deletion itself to complete.
         */
        ItemCompletionHandler COMPLETE_LISTENER_IMMEDIATELY = (ignoredUuid, listener) -> listener.onResponse(null);
    }
}
