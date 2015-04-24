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

package org.elasticsearch.action.admin.cluster.snapshots.delete;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for delete snapshot operation
 */
public class TransportDeleteSnapshotAction extends TransportMasterNodeOperationAction<DeleteSnapshotRequest, DeleteSnapshotResponse> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportDeleteSnapshotAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, SnapshotsService snapshotsService, ActionFilters actionFilters) {
        super(settings, DeleteSnapshotAction.NAME, transportService, clusterService, threadPool, actionFilters, DeleteSnapshotRequest.class);
        this.snapshotsService = snapshotsService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected DeleteSnapshotResponse newResponse() {
        return new DeleteSnapshotResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteSnapshotRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, "");
    }

    @Override
    protected void masterOperation(final DeleteSnapshotRequest request, ClusterState state, final ActionListener<DeleteSnapshotResponse> listener) throws ElasticsearchException {
        SnapshotId snapshotIds = new SnapshotId(request.repository(), request.snapshot());
        snapshotsService.deleteSnapshot(snapshotIds, new SnapshotsService.DeleteSnapshotListener() {
            @Override
            public void onResponse() {
                listener.onResponse(new DeleteSnapshotResponse(true));
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }
}
