/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TransportSnapshotsStatusAction extends TransportMasterNodeAction<SnapshotsStatusRequest, SnapshotsStatusResponse> {

    private static final Logger logger = LogManager.getLogger(TransportSnapshotsStatusAction.class);

    private final SnapshotsService snapshotsService;

    private final NodeClient client;

    @Inject
    public TransportSnapshotsStatusAction(TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, SnapshotsService snapshotsService, NodeClient client,
                                          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(SnapshotsStatusAction.NAME, transportService, clusterService, threadPool, actionFilters,
              SnapshotsStatusRequest::new, indexNameExpressionResolver);
        this.snapshotsService = snapshotsService;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected ClusterBlockException checkBlock(SnapshotsStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected SnapshotsStatusResponse read(StreamInput in) throws IOException {
        return new SnapshotsStatusResponse(in);
    }

    @Override
    protected void masterOperation(Task task, final SnapshotsStatusRequest request,
                                   final ClusterState state,
                                   final ActionListener<SnapshotsStatusResponse> listener) throws Exception {
        final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
        List<SnapshotsInProgress.Entry> currentSnapshots =
            SnapshotsService.currentSnapshots(snapshotsInProgress, request.repository(), Arrays.asList(request.snapshots()));
        if (currentSnapshots.isEmpty()) {
            buildResponse(snapshotsInProgress, request, currentSnapshots, null, listener);
            return;
        }

        Set<String> nodesIds = new HashSet<>();
        for (SnapshotsInProgress.Entry entry : currentSnapshots) {
            for (ObjectCursor<SnapshotsInProgress.ShardSnapshotStatus> status : entry.shards().values()) {
                if (status.value.nodeId() != null) {
                    nodesIds.add(status.value.nodeId());
                }
            }
        }

        if (!nodesIds.isEmpty()) {
            // There are still some snapshots running - check their progress
            Snapshot[] snapshots = new Snapshot[currentSnapshots.size()];
            for (int i = 0; i < currentSnapshots.size(); i++) {
                snapshots[i] = currentSnapshots.get(i).snapshot();
            }
            client.executeLocally(TransportNodesSnapshotsStatus.TYPE,
                new TransportNodesSnapshotsStatus.Request(nodesIds.toArray(Strings.EMPTY_ARRAY))
                    .snapshots(snapshots).timeout(request.masterNodeTimeout()),
                ActionListener.wrap(nodeSnapshotStatuses -> threadPool.generic().execute(
                    ActionRunnable.wrap(listener,
                        l -> buildResponse(snapshotsInProgress, request, currentSnapshots, nodeSnapshotStatuses, l))
                ), listener::onFailure));
        } else {
            // We don't have any in-progress shards, just return current stats
            buildResponse(snapshotsInProgress, request, currentSnapshots, null, listener);
        }

    }

    private void buildResponse(@Nullable SnapshotsInProgress snapshotsInProgress, SnapshotsStatusRequest request,
                               List<SnapshotsInProgress.Entry> currentSnapshotEntries,
                               TransportNodesSnapshotsStatus.NodesSnapshotStatus nodeSnapshotStatuses,
                               ActionListener<SnapshotsStatusResponse> listener) {
        // First process snapshot that are currently processed
        List<SnapshotStatus> builder = new ArrayList<>();
        Set<String> currentSnapshotNames = new HashSet<>();
        if (!currentSnapshotEntries.isEmpty()) {
            Map<String, TransportNodesSnapshotsStatus.NodeSnapshotStatus> nodeSnapshotStatusMap;
            if (nodeSnapshotStatuses != null) {
                nodeSnapshotStatusMap = nodeSnapshotStatuses.getNodesMap();
            } else {
                nodeSnapshotStatusMap = new HashMap<>();
            }

            for (SnapshotsInProgress.Entry entry : currentSnapshotEntries) {
                currentSnapshotNames.add(entry.snapshot().getSnapshotId().getName());
                List<SnapshotIndexShardStatus> shardStatusBuilder = new ArrayList<>();
                for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardEntry : entry.shards()) {
                    SnapshotsInProgress.ShardSnapshotStatus status = shardEntry.value;
                    if (status.nodeId() != null) {
                        // We should have information about this shard from the shard:
                        TransportNodesSnapshotsStatus.NodeSnapshotStatus nodeStatus = nodeSnapshotStatusMap.get(status.nodeId());
                        if (nodeStatus != null) {
                            Map<ShardId, SnapshotIndexShardStatus> shardStatues = nodeStatus.status().get(entry.snapshot());
                            if (shardStatues != null) {
                                SnapshotIndexShardStatus shardStatus = shardStatues.get(shardEntry.key);
                                if (shardStatus != null) {
                                    // We have full information about this shard
                                    shardStatusBuilder.add(shardStatus);
                                    continue;
                                }
                            }
                        }
                    }
                    final SnapshotIndexShardStage stage;
                    switch (shardEntry.value.state()) {
                        case FAILED:
                        case ABORTED:
                        case MISSING:
                            stage = SnapshotIndexShardStage.FAILURE;
                            break;
                        case INIT:
                        case WAITING:
                            stage = SnapshotIndexShardStage.STARTED;
                            break;
                        case SUCCESS:
                            stage = SnapshotIndexShardStage.DONE;
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown snapshot state " + shardEntry.value.state());
                    }
                    SnapshotIndexShardStatus shardStatus = new SnapshotIndexShardStatus(shardEntry.key, stage);
                    shardStatusBuilder.add(shardStatus);
                }
                builder.add(new SnapshotStatus(entry.snapshot(), entry.state(),
                    Collections.unmodifiableList(shardStatusBuilder), entry.includeGlobalState(), entry.startTime(),
                    Math.max(threadPool.absoluteTimeInMillis() - entry.startTime(), 0L)));
            }
        }
        // Now add snapshots on disk that are not currently running
        final String repositoryName = request.repository();
        if (Strings.hasText(repositoryName) && request.snapshots() != null && request.snapshots().length > 0) {
            loadRepositoryData(snapshotsInProgress, request, builder, currentSnapshotNames, repositoryName, listener);
        } else {
            listener.onResponse(new SnapshotsStatusResponse(Collections.unmodifiableList(builder)));
        }
    }

    private void loadRepositoryData(@Nullable SnapshotsInProgress snapshotsInProgress, SnapshotsStatusRequest request,
                                    List<SnapshotStatus> builder, Set<String> currentSnapshotNames, String repositoryName,
                                    ActionListener<SnapshotsStatusResponse> listener) {
        final Set<String> requestedSnapshotNames = Sets.newHashSet(request.snapshots());
        final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
        snapshotsService.getRepositoryData(repositoryName, repositoryDataListener);
        repositoryDataListener.whenComplete(repositoryData -> {
            final Map<String, SnapshotId> matchedSnapshotIds = repositoryData.getSnapshotIds().stream()
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
                        logger.debug("snapshot status request ignoring snapshot [{}], not found in repository [{}]",
                                     snapshotName, repositoryName);
                        continue;
                    } else {
                        throw new SnapshotMissingException(repositoryName, snapshotName);
                    }
                }
                SnapshotInfo snapshotInfo = snapshotsService.snapshot(snapshotsInProgress, repositoryName, snapshotId);
                List<SnapshotIndexShardStatus> shardStatusBuilder = new ArrayList<>();
                if (snapshotInfo.state().completed()) {
                    Map<ShardId, IndexShardSnapshotStatus> shardStatuses =
                        snapshotsService.snapshotShards(repositoryName, repositoryData, snapshotInfo);
                    for (Map.Entry<ShardId, IndexShardSnapshotStatus> shardStatus : shardStatuses.entrySet()) {
                        IndexShardSnapshotStatus.Copy lastSnapshotStatus = shardStatus.getValue().asCopy();
                        shardStatusBuilder.add(new SnapshotIndexShardStatus(shardStatus.getKey(), lastSnapshotStatus));
                    }
                    final SnapshotsInProgress.State state;
                    switch (snapshotInfo.state()) {
                        case FAILED:
                            state = SnapshotsInProgress.State.FAILED;
                            break;
                        case SUCCESS:
                        case PARTIAL:
                            // Translating both PARTIAL and SUCCESS to SUCCESS for now
                            // TODO: add the differentiation on the metadata level in the next major release
                            state = SnapshotsInProgress.State.SUCCESS;
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown snapshot state " + snapshotInfo.state());
                    }
                    final long startTime = snapshotInfo.startTime();
                    final long endTime = snapshotInfo.endTime();
                    assert endTime >= startTime || (endTime == 0L && snapshotInfo.state().completed() == false)
                        : "Inconsistent timestamps found in SnapshotInfo [" + snapshotInfo + "]";
                    builder.add(new SnapshotStatus(new Snapshot(repositoryName, snapshotId), state,
                        Collections.unmodifiableList(shardStatusBuilder), snapshotInfo.includeGlobalState(),
                        startTime,
                        // Use current time to calculate overall runtime for in-progress snapshots that have endTime == 0
                        (endTime == 0 ? threadPool.absoluteTimeInMillis() : endTime) - startTime));
                }
            }
            listener.onResponse(new SnapshotsStatusResponse(Collections.unmodifiableList(builder)));
        }, listener::onFailure);
    }

}
