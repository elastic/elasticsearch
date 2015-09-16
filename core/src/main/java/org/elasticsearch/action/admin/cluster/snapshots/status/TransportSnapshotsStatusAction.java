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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class TransportSnapshotsStatusAction extends TransportMasterNodeAction<SnapshotsStatusRequest, SnapshotsStatusResponse> {

    private final SnapshotsService snapshotsService;

    private final TransportNodesSnapshotsStatus transportNodesSnapshotsStatus;

    @Inject
    public TransportSnapshotsStatusAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, SnapshotsService snapshotsService,
                                          TransportNodesSnapshotsStatus transportNodesSnapshotsStatus,
                                          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, SnapshotsStatusAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, SnapshotsStatusRequest::new);
        this.snapshotsService = snapshotsService;
        this.transportNodesSnapshotsStatus = transportNodesSnapshotsStatus;
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
    protected SnapshotsStatusResponse newResponse() {
        return new SnapshotsStatusResponse();
    }

    @Override
    protected void masterOperation(final SnapshotsStatusRequest request,
                                   final ClusterState state,
                                   final ActionListener<SnapshotsStatusResponse> listener) throws Exception {
        List<SnapshotsInProgress.Entry> currentSnapshots = snapshotsService.currentSnapshots(request.repository(), request.snapshots());

        if (currentSnapshots.isEmpty()) {
            listener.onResponse(buildResponse(request, currentSnapshots, null));
            return;
        }

        Set<String> nodesIds = new HashSet<>();
        for (SnapshotsInProgress.Entry entry : currentSnapshots) {
            for (SnapshotsInProgress.ShardSnapshotStatus status : entry.shards().values()) {
                if (status.nodeId() != null) {
                    nodesIds.add(status.nodeId());
                }
            }
        }

        if (!nodesIds.isEmpty()) {
            // There are still some snapshots running - check their progress
            SnapshotId[] snapshotIds = new SnapshotId[currentSnapshots.size()];
            for (int i = 0; i < currentSnapshots.size(); i++) {
                snapshotIds[i] = currentSnapshots.get(i).snapshotId();
            }

            TransportNodesSnapshotsStatus.Request nodesRequest = new TransportNodesSnapshotsStatus.Request(request, nodesIds.toArray(new String[nodesIds.size()]))
                    .snapshotIds(snapshotIds).timeout(request.masterNodeTimeout());
            transportNodesSnapshotsStatus.execute(nodesRequest, new ActionListener<TransportNodesSnapshotsStatus.NodesSnapshotStatus>() {
                        @Override
                        public void onResponse(TransportNodesSnapshotsStatus.NodesSnapshotStatus nodeSnapshotStatuses) {
                            try {
                                List<SnapshotsInProgress.Entry> currentSnapshots =
                                        snapshotsService.currentSnapshots(request.repository(), request.snapshots());
                                listener.onResponse(buildResponse(request, currentSnapshots, nodeSnapshotStatuses));
                            } catch (Throwable e) {
                                listener.onFailure(e);
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            listener.onFailure(e);
                        }
                    });
        } else {
            // We don't have any in-progress shards, just return current stats
            listener.onResponse(buildResponse(request, currentSnapshots, null));
        }

    }

    private SnapshotsStatusResponse buildResponse(SnapshotsStatusRequest request, List<SnapshotsInProgress.Entry> currentSnapshots,
                                                  TransportNodesSnapshotsStatus.NodesSnapshotStatus nodeSnapshotStatuses) throws IOException {
        // First process snapshot that are currently processed
        List<SnapshotStatus> builder = new ArrayList<>();
        Set<SnapshotId> currentSnapshotIds = new HashSet<>();
        if (!currentSnapshots.isEmpty()) {
            Map<String, TransportNodesSnapshotsStatus.NodeSnapshotStatus> nodeSnapshotStatusMap;
            if (nodeSnapshotStatuses != null) {
                nodeSnapshotStatusMap = nodeSnapshotStatuses.getNodesMap();
            } else {
                nodeSnapshotStatusMap = new HashMap<>();
            }

            for (SnapshotsInProgress.Entry entry : currentSnapshots) {
                currentSnapshotIds.add(entry.snapshotId());
                List<SnapshotIndexShardStatus> shardStatusBuilder = new ArrayList<>();
                for (ImmutableMap.Entry<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardEntry : entry.shards().entrySet()) {
                    SnapshotsInProgress.ShardSnapshotStatus status = shardEntry.getValue();
                    if (status.nodeId() != null) {
                        // We should have information about this shard from the shard:
                        TransportNodesSnapshotsStatus.NodeSnapshotStatus nodeStatus = nodeSnapshotStatusMap.get(status.nodeId());
                        if (nodeStatus != null) {
                            ImmutableMap<ShardId, SnapshotIndexShardStatus> shardStatues = nodeStatus.status().get(entry.snapshotId());
                            if (shardStatues != null) {
                                SnapshotIndexShardStatus shardStatus = shardStatues.get(shardEntry.getKey());
                                if (shardStatus != null) {
                                    // We have full information about this shard
                                    shardStatusBuilder.add(shardStatus);
                                    continue;
                                }
                            }
                        }
                    }
                    final SnapshotIndexShardStage stage;
                    switch (shardEntry.getValue().state()) {
                        case FAILED:
                        case ABORTED:
                        case MISSING:
                            stage = SnapshotIndexShardStage.FAILURE;
                            break;
                        case INIT:
                        case WAITING:
                        case STARTED:
                            stage = SnapshotIndexShardStage.STARTED;
                            break;
                        case SUCCESS:
                            stage = SnapshotIndexShardStage.DONE;
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown snapshot state " + shardEntry.getValue().state());
                    }
                    SnapshotIndexShardStatus shardStatus = new SnapshotIndexShardStatus(shardEntry.getKey(), stage);
                    shardStatusBuilder.add(shardStatus);
                }
                builder.add(new SnapshotStatus(entry.snapshotId(), entry.state(), Collections.unmodifiableList(shardStatusBuilder)));
            }
        }
        // Now add snapshots on disk that are not currently running
        if (Strings.hasText(request.repository())) {
            if (request.snapshots() != null && request.snapshots().length > 0) {
                for (String snapshotName : request.snapshots()) {
                    SnapshotId snapshotId = new SnapshotId(request.repository(), snapshotName);
                    if (currentSnapshotIds.contains(snapshotId)) {
                        // This is a snapshot the is currently running - skipping
                        continue;
                    }
                    Snapshot snapshot = snapshotsService.snapshot(snapshotId);
                    List<SnapshotIndexShardStatus> shardStatusBuilder = new ArrayList<>();
                    if (snapshot.state().completed()) {
                        ImmutableMap<ShardId, IndexShardSnapshotStatus> shardStatues = snapshotsService.snapshotShards(snapshotId);
                        for (ImmutableMap.Entry<ShardId, IndexShardSnapshotStatus> shardStatus : shardStatues.entrySet()) {
                            shardStatusBuilder.add(new SnapshotIndexShardStatus(shardStatus.getKey(), shardStatus.getValue()));
                        }
                        final SnapshotsInProgress.State state;
                        switch (snapshot.state()) {
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
                                throw new IllegalArgumentException("Unknown snapshot state " + snapshot.state());
                        }
                        builder.add(new SnapshotStatus(snapshotId, state, Collections.unmodifiableList(shardStatusBuilder)));
                    }
                }
            }
        }

        return new SnapshotsStatusResponse(Collections.unmodifiableList(builder));
    }

}
