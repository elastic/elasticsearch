/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountAwareThreadedActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.cluster.SnapshotsInProgress.ShardState.SUCCESS;

public class TransportSnapshotsStatusAction extends TransportMasterNodeAction<SnapshotsStatusRequest, SnapshotsStatusResponse> {

    public static final ActionType<SnapshotsStatusResponse> TYPE = new ActionType<>("cluster:admin/snapshot/status");
    private static final Logger logger = LogManager.getLogger(TransportSnapshotsStatusAction.class);

    private final RepositoriesService repositoriesService;

    private final NodeClient client;

    @Inject
    public TransportSnapshotsStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        RepositoriesService repositoriesService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            SnapshotsStatusRequest::new,
            indexNameExpressionResolver,
            SnapshotsStatusResponse::new,
            // building the response is somewhat expensive for large snapshots, so we fork.
            threadPool.executor(ThreadPool.Names.SNAPSHOT_META)
        );
        this.repositoriesService = repositoriesService;
        this.client = client;
    }

    @Override
    protected ClusterBlockException checkBlock(SnapshotsStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(
        Task task,
        final SnapshotsStatusRequest request,
        final ClusterState state,
        final ActionListener<SnapshotsStatusResponse> listener
    ) throws Exception {
        assert task instanceof CancellableTask : task + " not cancellable";
        final CancellableTask cancellableTask = (CancellableTask) task;

        final SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(state);
        List<SnapshotsInProgress.Entry> currentSnapshots = SnapshotsService.currentSnapshots(
            snapshotsInProgress,
            request.repository(),
            Arrays.asList(request.snapshots())
        );
        if (currentSnapshots.isEmpty()) {
            buildResponse(snapshotsInProgress, request, currentSnapshots, null, cancellableTask, listener);
            return;
        }

        Set<String> nodesIds = new HashSet<>();
        for (SnapshotsInProgress.Entry entry : currentSnapshots) {
            for (SnapshotsInProgress.ShardSnapshotStatus status : entry.shardSnapshotStatusByRepoShardId().values()) {
                if (status.nodeId() != null) {
                    nodesIds.add(status.nodeId());
                }
            }
        }

        if (nodesIds.isEmpty() == false) {
            // There are still some snapshots running - check their progress
            Snapshot[] snapshots = new Snapshot[currentSnapshots.size()];
            for (int i = 0; i < currentSnapshots.size(); i++) {
                snapshots[i] = currentSnapshots.get(i).snapshot();
            }
            client.executeLocally(
                TransportNodesSnapshotsStatus.TYPE,
                new TransportNodesSnapshotsStatus.Request(nodesIds.toArray(Strings.EMPTY_ARRAY)).snapshots(snapshots)
                    .timeout(request.masterNodeTimeout().millis() < 0 ? null : request.masterNodeTimeout()),
                // fork to snapshot meta since building the response is expensive for large snapshots
                new RefCountAwareThreadedActionListener<>(
                    threadPool.executor(ThreadPool.Names.SNAPSHOT_META),
                    listener.delegateFailureAndWrap(
                        (l, nodeSnapshotStatuses) -> buildResponse(
                            snapshotsInProgress,
                            request,
                            currentSnapshots,
                            nodeSnapshotStatuses,
                            cancellableTask,
                            l
                        )
                    )
                )
            );
        } else {
            // We don't have any in-progress shards, just return current stats
            buildResponse(snapshotsInProgress, request, currentSnapshots, null, cancellableTask, listener);
        }

    }

    private void buildResponse(
        SnapshotsInProgress snapshotsInProgress,
        SnapshotsStatusRequest request,
        List<SnapshotsInProgress.Entry> currentSnapshotEntries,
        TransportNodesSnapshotsStatus.NodesSnapshotStatus nodeSnapshotStatuses,
        CancellableTask task,
        ActionListener<SnapshotsStatusResponse> listener
    ) {
        // First process snapshot that are currently processed
        List<SnapshotStatus> builder = new ArrayList<>();
        Set<String> currentSnapshotNames = new HashSet<>();
        if (currentSnapshotEntries.isEmpty() == false) {
            Map<String, TransportNodesSnapshotsStatus.NodeSnapshotStatus> nodeSnapshotStatusMap;
            if (nodeSnapshotStatuses != null) {
                nodeSnapshotStatusMap = nodeSnapshotStatuses.getNodesMap();
            } else {
                nodeSnapshotStatusMap = new HashMap<>();
            }

            for (SnapshotsInProgress.Entry entry : currentSnapshotEntries) {
                currentSnapshotNames.add(entry.snapshot().getSnapshotId().getName());
                List<SnapshotIndexShardStatus> shardStatusBuilder = new ArrayList<>();
                for (Map.Entry<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> shardEntry : entry
                    .shardSnapshotStatusByRepoShardId()
                    .entrySet()) {
                    SnapshotsInProgress.ShardSnapshotStatus status = shardEntry.getValue();
                    if (status.nodeId() != null) {
                        // We should have information about this shard from the shard:
                        TransportNodesSnapshotsStatus.NodeSnapshotStatus nodeStatus = nodeSnapshotStatusMap.get(status.nodeId());
                        if (nodeStatus != null) {
                            Map<ShardId, SnapshotIndexShardStatus> shardStatues = nodeStatus.status().get(entry.snapshot());
                            if (shardStatues != null) {
                                final ShardId sid = entry.shardId(shardEntry.getKey());
                                SnapshotIndexShardStatus shardStatus = shardStatues.get(sid);
                                if (shardStatus != null) {
                                    // We have full information about this shard
                                    if (shardStatus.getStage() == SnapshotIndexShardStage.DONE
                                        && shardEntry.getValue().state() != SUCCESS) {
                                        // Unlikely edge case:
                                        // Data node has finished snapshotting the shard but the cluster state has not yet been updated
                                        // to reflect this. We adjust the status to show up as snapshot metadata being written because
                                        // technically if the data node failed before successfully reporting DONE state to master, then
                                        // this shards state would jump to a failed state.
                                        shardStatus = new SnapshotIndexShardStatus(
                                            sid,
                                            SnapshotIndexShardStage.FINALIZE,
                                            shardStatus.getStats(),
                                            shardStatus.getNodeId(),
                                            shardStatus.getFailure()
                                        );
                                    }
                                    shardStatusBuilder.add(shardStatus);
                                    continue;
                                }
                            }
                        }
                    }
                    // We failed to find the status of the shard from the responses we received from data nodes.
                    // This can happen if nodes drop out of the cluster completely or restart during the snapshot.
                    // We rebuild the information they would have provided from their in memory state from the cluster
                    // state and the repository contents in the below logic
                    final SnapshotIndexShardStage stage = switch (shardEntry.getValue().state()) {
                        case FAILED, ABORTED, MISSING -> SnapshotIndexShardStage.FAILURE;
                        case INIT, WAITING, PAUSED_FOR_NODE_REMOVAL, QUEUED -> SnapshotIndexShardStage.STARTED;
                        case SUCCESS -> SnapshotIndexShardStage.DONE;
                    };
                    final SnapshotIndexShardStatus shardStatus;
                    if (stage == SnapshotIndexShardStage.DONE) {
                        // Shard snapshot completed successfully so we should be able to load the exact statistics for this
                        // shard from the repository already.
                        final ShardId shardId = entry.shardId(shardEntry.getKey());
                        shardStatus = new SnapshotIndexShardStatus(
                            shardId,
                            repositoriesService.repository(entry.repository())
                                .getShardSnapshotStatus(
                                    entry.snapshot().getSnapshotId(),
                                    entry.indices().get(shardId.getIndexName()),
                                    shardId
                                )
                        );
                    } else {
                        shardStatus = new SnapshotIndexShardStatus(entry.shardId(shardEntry.getKey()), stage);
                    }
                    shardStatusBuilder.add(shardStatus);
                }
                builder.add(
                    new SnapshotStatus(
                        entry.snapshot(),
                        entry.state(),
                        Collections.unmodifiableList(shardStatusBuilder),
                        entry.includeGlobalState(),
                        entry.startTime(),
                        Math.max(threadPool.absoluteTimeInMillis() - entry.startTime(), 0L)
                    )
                );
            }
        }
        // Now add snapshots on disk that are not currently running
        final String repositoryName = request.repository();
        if (Strings.hasText(repositoryName) && CollectionUtils.isEmpty(request.snapshots()) == false) {
            loadRepositoryData(snapshotsInProgress, request, builder, currentSnapshotNames, repositoryName, task, listener);
        } else {
            listener.onResponse(new SnapshotsStatusResponse(Collections.unmodifiableList(builder)));
        }
    }

    private void loadRepositoryData(
        SnapshotsInProgress snapshotsInProgress,
        SnapshotsStatusRequest request,
        List<SnapshotStatus> builder,
        Set<String> currentSnapshotNames,
        String repositoryName,
        CancellableTask task,
        ActionListener<SnapshotsStatusResponse> listener
    ) {
        final Set<String> requestedSnapshotNames = Sets.newHashSet(request.snapshots());
        final ListenableFuture<RepositoryData> repositoryDataListener = new ListenableFuture<>();
        repositoriesService.getRepositoryData(repositoryName, repositoryDataListener);
        final Collection<SnapshotId> snapshotIdsToLoad = new ArrayList<>();
        repositoryDataListener.addListener(listener.delegateFailureAndWrap((delegate, repositoryData) -> {
            task.ensureNotCancelled();
            final Map<String, SnapshotId> matchedSnapshotIds = repositoryData.getSnapshotIds()
                .stream()
                .filter(s -> requestedSnapshotNames.contains(s.getName()))
                .collect(Collectors.toMap(SnapshotId::getName, Function.identity()));
            for (final String snapshotName : request.snapshots()) {
                if (currentSnapshotNames.contains(snapshotName)) {
                    // we've already found this snapshot in the current snapshot entries, so skip over
                    continue;
                }
                SnapshotId snapshotId = matchedSnapshotIds.get(snapshotName);
                if (snapshotId == null) {
                    // neither in the current snapshot entries nor found in the repository
                    if (request.ignoreUnavailable()) {
                        // ignoring unavailable snapshots, so skip over
                        logger.debug(
                            "snapshot status request ignoring snapshot [{}], not found in repository [{}]",
                            snapshotName,
                            repositoryName
                        );
                        continue;
                    } else {
                        throw new SnapshotMissingException(repositoryName, snapshotName);
                    }
                }
                if (snapshotsInProgress.snapshot(new Snapshot(repositoryName, snapshotId)) == null) {
                    snapshotIdsToLoad.add(snapshotId);
                }
            }

            if (snapshotIdsToLoad.isEmpty()) {
                delegate.onResponse(new SnapshotsStatusResponse(Collections.unmodifiableList(builder)));
            } else {
                final List<SnapshotStatus> threadSafeBuilder = Collections.synchronizedList(builder);
                repositoriesService.repository(repositoryName).getSnapshotInfo(snapshotIdsToLoad, true, task::isCancelled, snapshotInfo -> {
                    List<SnapshotIndexShardStatus> shardStatusBuilder = new ArrayList<>();
                    final Map<ShardId, IndexShardSnapshotStatus.Copy> shardStatuses;
                    shardStatuses = snapshotShards(repositoryName, repositoryData, task, snapshotInfo);
                    // an exception here stops further fetches of snapshotInfo since context is fail-fast
                    for (final var shardStatus : shardStatuses.entrySet()) {
                        IndexShardSnapshotStatus.Copy lastSnapshotStatus = shardStatus.getValue();
                        shardStatusBuilder.add(new SnapshotIndexShardStatus(shardStatus.getKey(), lastSnapshotStatus));
                    }
                    final SnapshotsInProgress.State state = switch (snapshotInfo.state()) {
                        case FAILED -> SnapshotsInProgress.State.FAILED;
                        case SUCCESS, PARTIAL ->
                            // Both of these means the snapshot has completed.
                            SnapshotsInProgress.State.SUCCESS;
                        default -> throw new IllegalArgumentException("Unexpected snapshot state " + snapshotInfo.state());
                    };
                    final long startTime = snapshotInfo.startTime();
                    final long endTime = snapshotInfo.endTime();
                    assert endTime >= startTime || (endTime == 0L && snapshotInfo.state().completed() == false)
                        : "Inconsistent timestamps found in SnapshotInfo [" + snapshotInfo + "]";
                    threadSafeBuilder.add(
                        new SnapshotStatus(
                            new Snapshot(repositoryName, snapshotInfo.snapshotId()),
                            state,
                            Collections.unmodifiableList(shardStatusBuilder),
                            snapshotInfo.includeGlobalState(),
                            startTime,
                            // Use current time to calculate overall runtime for in-progress snapshots that have endTime == 0
                            (endTime == 0 ? threadPool.absoluteTimeInMillis() : endTime) - startTime
                        )
                    );
                }, delegate.map(v -> new SnapshotsStatusResponse(List.copyOf(threadSafeBuilder))));
            }
        }));
    }

    /**
     * Returns status of shards currently finished snapshots
     * <p>
     * This method is executed on master node and it's complimentary to the
     * {@link SnapshotShardsService#currentSnapshotShards(Snapshot)} because it
     * returns similar information but for already finished snapshots.
     * </p>
     *
     * @param repositoryName  repository name
     * @param snapshotInfo    snapshot info
     * @return map of shard id to snapshot status
     */
    private Map<ShardId, IndexShardSnapshotStatus.Copy> snapshotShards(
        final String repositoryName,
        final RepositoryData repositoryData,
        final CancellableTask task,
        final SnapshotInfo snapshotInfo
    ) throws IOException {
        final Repository repository = repositoriesService.repository(repositoryName);
        final Map<ShardId, IndexShardSnapshotStatus.Copy> shardStatus = new HashMap<>();
        for (String index : snapshotInfo.indices()) {
            IndexId indexId = repositoryData.resolveIndexId(index);
            task.ensureNotCancelled();
            IndexMetadata indexMetadata = repository.getSnapshotIndexMetaData(repositoryData, snapshotInfo.snapshotId(), indexId);
            if (indexMetadata != null) {
                int numberOfShards = indexMetadata.getNumberOfShards();
                for (int i = 0; i < numberOfShards; i++) {
                    ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
                    SnapshotShardFailure shardFailure = findShardFailure(snapshotInfo.shardFailures(), shardId);
                    if (shardFailure != null) {
                        shardStatus.put(shardId, IndexShardSnapshotStatus.newFailed(shardFailure.reason()));
                    } else {
                        final IndexShardSnapshotStatus.Copy shardSnapshotStatus;
                        if (snapshotInfo.state() == SnapshotState.FAILED) {
                            // If the snapshot failed, but the shard's snapshot does
                            // not have an exception, it means that partial snapshots
                            // were disabled and in this case, the shard snapshot will
                            // *not* have any metadata, so attempting to read the shard
                            // snapshot status will throw an exception. Instead, we create
                            // a status for the shard to indicate that the shard snapshot
                            // could not be taken due to partial being set to false.
                            shardSnapshotStatus = IndexShardSnapshotStatus.newFailed("skipped");
                        } else {
                            task.ensureNotCancelled();
                            shardSnapshotStatus = repository.getShardSnapshotStatus(snapshotInfo.snapshotId(), indexId, shardId);
                        }
                        shardStatus.put(shardId, shardSnapshotStatus);
                    }
                }
            }
        }
        return unmodifiableMap(shardStatus);
    }

    private static SnapshotShardFailure findShardFailure(List<SnapshotShardFailure> shardFailures, ShardId shardId) {
        for (SnapshotShardFailure shardFailure : shardFailures) {
            if (shardId.getIndexName().equals(shardFailure.index()) && shardId.getId() == shardFailure.shardId()) {
                return shardFailure;
            }
        }
        return null;
    }
}
