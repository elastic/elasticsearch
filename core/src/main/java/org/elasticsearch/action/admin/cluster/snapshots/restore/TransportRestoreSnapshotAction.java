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

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for restore snapshot operation
 */
public class TransportRestoreSnapshotAction extends TransportMasterNodeAction<RestoreSnapshotRequest, RestoreSnapshotResponse> {
    private final RestoreService restoreService;

    @Inject
    public TransportRestoreSnapshotAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, RestoreService restoreService, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, RestoreSnapshotAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, RestoreSnapshotRequest::new);
        this.restoreService = restoreService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SNAPSHOT;
    }

    @Override
    protected RestoreSnapshotResponse newResponse() {
        return new RestoreSnapshotResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(RestoreSnapshotRequest request, ClusterState state) {
        // Restoring a snapshot might change the global state and create/change an index,
        // so we need to check for METADATA_WRITE and WRITE blocks
        ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (blockException != null) {
            return blockException;
        }
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);

    }

    @Override
    protected void masterOperation(final RestoreSnapshotRequest request, final ClusterState state, final ActionListener<RestoreSnapshotResponse> listener) {
        RestoreService.RestoreRequest restoreRequest = new RestoreService.RestoreRequest(request.repository(), request.snapshot(),
                request.indices(), request.indicesOptions(), request.renamePattern(), request.renameReplacement(),
                request.settings(), request.masterNodeTimeout(), request.includeGlobalState(), request.partial(), request.includeAliases(),
                request.indexSettings(), request.ignoreIndexSettings(), "restore_snapshot[" + request.snapshot() + "]");

        restoreService.restoreSnapshot(restoreRequest, new ActionListener<RestoreInfo>() {
            @Override
            public void onResponse(RestoreInfo restoreInfo) {
                if (restoreInfo == null && request.waitForCompletion()) {
                    restoreService.addListener(new ActionListener<RestoreService.RestoreCompletionResponse>() {
                        @Override
                        public void onResponse(RestoreService.RestoreCompletionResponse restoreCompletionResponse) {
                            final Snapshot snapshot = restoreCompletionResponse.getSnapshot();
                            if (snapshot.getRepository().equals(request.repository()) &&
                                    snapshot.getSnapshotId().getName().equals(request.snapshot())) {
                                listener.onResponse(new RestoreSnapshotResponse(restoreCompletionResponse.getRestoreInfo()));
                                restoreService.removeListener(this);
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            listener.onFailure(e);
                        }
                    });
                } else {
                    listener.onResponse(new RestoreSnapshotResponse(restoreInfo));
                }
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }
}
