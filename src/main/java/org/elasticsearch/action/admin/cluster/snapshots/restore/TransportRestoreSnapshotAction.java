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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.RestoreService.RestoreSnapshotListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for restore snapshot operation
 */
public class TransportRestoreSnapshotAction extends TransportMasterNodeOperationAction<RestoreSnapshotRequest, RestoreSnapshotResponse> {
    private final RestoreService restoreService;

    @Inject
    public TransportRestoreSnapshotAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, RestoreService restoreService) {
        super(settings, transportService, clusterService, threadPool);
        this.restoreService = restoreService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SNAPSHOT;
    }

    @Override
    protected String transportAction() {
        return RestoreSnapshotAction.NAME;
    }

    @Override
    protected RestoreSnapshotRequest newRequest() {
        return new RestoreSnapshotRequest();
    }

    @Override
    protected RestoreSnapshotResponse newResponse() {
        return new RestoreSnapshotResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(RestoreSnapshotRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
    }

    @Override
    protected void masterOperation(final RestoreSnapshotRequest request, ClusterState state, final ActionListener<RestoreSnapshotResponse> listener) throws ElasticsearchException {
        RestoreService.RestoreRequest restoreRequest = new RestoreService.RestoreRequest(
                "restore_snapshot[" + request.snapshot() + "]", request.repository(), request.snapshot(),
                request.indices(), request.indicesOptions(), request.renamePattern(), request.renameReplacement(),
                request.settings(), request.masterNodeTimeout(), request.includeGlobalState(), request.partial());
        restoreService.restoreSnapshot(restoreRequest, new RestoreSnapshotListener() {
            @Override
            public void onResponse(RestoreInfo restoreInfo) {
                if (restoreInfo == null) {
                    if (request.waitForCompletion()) {
                        restoreService.addListener(new RestoreService.RestoreCompletionListener() {
                            SnapshotId snapshotId = new SnapshotId(request.repository(), request.snapshot());

                            @Override
                            public void onRestoreCompletion(SnapshotId snapshotId, RestoreInfo snapshot) {
                                if (this.snapshotId.equals(snapshotId)) {
                                    listener.onResponse(new RestoreSnapshotResponse(snapshot));
                                    restoreService.removeListener(this);
                                }
                            }
                        });
                    } else {
                        listener.onResponse(new RestoreSnapshotResponse(null));
                    }
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
