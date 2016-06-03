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
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for delete snapshot operation
 */
public class TransportDeleteSnapshotAction extends TransportMasterNodeAction<DeleteSnapshotRequest, DeleteSnapshotResponse> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportDeleteSnapshotAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, SnapshotsService snapshotsService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, DeleteSnapshotAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, DeleteSnapshotRequest::new);
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
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(final DeleteSnapshotRequest request, ClusterState state, final ActionListener<DeleteSnapshotResponse> listener) {
        snapshotsService.deleteSnapshot(request.repository(), request.snapshot(), new SnapshotsService.DeleteSnapshotListener() {
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
