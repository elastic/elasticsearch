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

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.cluster.SnapshotsInProgress.completed;

/**
 * A {@link TransportSnapshotUpdateStatusAction} receives snapshot state messages from {@link SnapshotShardsService},
 * then computes and updates the {@link ClusterState}.
 */
public class TransportSnapshotUpdateStatusAction extends TransportMasterNodeAction<UpdateSnapshotStatusRequest,
                                                                                   UpdateSnapshotStatusResponse> {
    private final SnapshotUpdateStateExecutor snapshotStateExecutor;

    @Inject
    public TransportSnapshotUpdateStatusAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                               ThreadPool threadPool, IndexNameExpressionResolver indexNameExpressionResolver,
                                               ActionFilters actionFilters, SnapshotsService snapshotsService) {
        super(settings, UpdateSnapshotStatusAction.NAME, transportService, clusterService,
            threadPool, actionFilters, indexNameExpressionResolver, UpdateSnapshotStatusRequest::new);
        this.snapshotStateExecutor = new SnapshotUpdateStateExecutor(snapshotsService, logger);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected UpdateSnapshotStatusResponse newResponse() {
        return new UpdateSnapshotStatusResponse();
    }

    @Override
    protected void masterOperation(UpdateSnapshotStatusRequest request, ClusterState state,
                                   ActionListener<UpdateSnapshotStatusResponse> listener) throws Exception {
        innerUpdateSnapshotState(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateSnapshotStatusRequest request, ClusterState state) {
        return null;
    }

    void innerUpdateSnapshotState(final UpdateSnapshotStatusRequest request, ActionListener<UpdateSnapshotStatusResponse> listener) {
        logger.trace((Supplier<?>) () -> new ParameterizedMessage("received updated snapshot restore status [{}]", request));
        clusterService.submitStateUpdateTask(
            "update snapshot state",
            request,
            ClusterStateTaskConfig.build(Priority.NORMAL),
            snapshotStateExecutor,
            new ClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    logger.error((Supplier<?>) () -> new ParameterizedMessage(
                        "unexpected failure while updating snapshot status [{}]", request), e);
                    try {
                        listener.onFailure(e);
                    } catch (Exception channelException) {
                        channelException.addSuppressed(e);
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage(
                            "failed to send failure [{}]  while updating snapshot status [{}]", e, request), channelException);
                    }
                }

                @Override
                public void onNoLongerMaster(String source) {
                    logger.error("no longer master while updating snapshot status [{}]", request);
                    try {
                        listener.onFailure(new NotMasterException(source));
                    } catch (Exception channelException) {
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage(
                            "{} failed to send no longer master updating snapshot[{}]", request.snapshot(), request), channelException);
                    }
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    try {
                        listener.onResponse(new UpdateSnapshotStatusResponse());
                    } catch (Exception channelException) {
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage(
                            "failed to send response after updating snapshot status [{}]", request), channelException);
                    }
                }
            }
        );
    }

    // The client node sends the update message to the master, then the master node updates the ClusterState.
    static class SnapshotUpdateStateExecutor implements ClusterStateTaskExecutor<UpdateSnapshotStatusRequest> {
        private final SnapshotsService snapshotsService;
        private final Logger logger;

        SnapshotUpdateStateExecutor(SnapshotsService snapshotsService, Logger logger) {
            this.snapshotsService = snapshotsService;
            this.logger = logger;
        }

        @Override
        public ClusterTasksResult<UpdateSnapshotStatusRequest> execute(ClusterState currentState,
                                                                       List<UpdateSnapshotStatusRequest> tasks) throws Exception {
            final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
            if (snapshots != null) {
                int changedCount = 0;
                final List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                    ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
                    boolean updated = false;

                    for (UpdateSnapshotStatusRequest updateSnapshotState : tasks) {
                        if (entry.snapshot().equals(updateSnapshotState.snapshot())) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("[{}] Updating shard [{}] with status [{}]",
                                    updateSnapshotState.snapshot(), updateSnapshotState.shardId(), updateSnapshotState.status().state());
                            }
                            if (updated == false) {
                                shards.putAll(entry.shards());
                                updated = true;
                            }
                            shards.put(updateSnapshotState.shardId(), updateSnapshotState.status());
                            changedCount++;
                        }
                    }

                    if (updated) {
                        if (completed(shards.values()) == false) {
                            entries.add(new SnapshotsInProgress.Entry(entry, shards.build()));
                        } else {
                            // Snapshot is finished - mark it as done
                            // TODO: Add PARTIAL_SUCCESS status?
                            SnapshotsInProgress.Entry updatedEntry = new SnapshotsInProgress.Entry(entry,
                                SnapshotsInProgress.State.SUCCESS, shards.build());
                            entries.add(updatedEntry);
                            // Finalize snapshot in the repository
                            snapshotsService.endSnapshot(updatedEntry);
                            logger.info("snapshot [{}] is done", updatedEntry.snapshot());
                        }
                    } else {
                        entries.add(entry);
                    }
                }
                if (changedCount > 0) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("changed cluster state triggered by {} snapshot state updates", changedCount);
                    }
                    final SnapshotsInProgress updatedSnapshots = new SnapshotsInProgress(
                        entries.toArray(new SnapshotsInProgress.Entry[entries.size()]));
                    return ClusterTasksResult.<UpdateSnapshotStatusRequest>builder().successes(tasks).build(
                        ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, updatedSnapshots).build());
                }
            }
            return ClusterTasksResult.<UpdateSnapshotStatusRequest>builder().successes(tasks).build(currentState);
        }
    }

}
