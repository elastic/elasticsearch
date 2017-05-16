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

package org.elasticsearch.legacy.action.admin.cluster.snapshots.get;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.block.ClusterBlockException;
import org.elasticsearch.legacy.cluster.block.ClusterBlockLevel;
import org.elasticsearch.legacy.cluster.metadata.SnapshotId;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.snapshots.Snapshot;
import org.elasticsearch.legacy.snapshots.SnapshotInfo;
import org.elasticsearch.legacy.snapshots.SnapshotsService;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

/**
 * Transport Action for get snapshots operation
 */
public class TransportGetSnapshotsAction extends TransportMasterNodeOperationAction<GetSnapshotsRequest, GetSnapshotsResponse> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportGetSnapshotsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, SnapshotsService snapshotsService) {
        super(settings, GetSnapshotsAction.NAME, transportService, clusterService, threadPool);
        this.snapshotsService = snapshotsService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SNAPSHOT;
    }

    @Override
    protected GetSnapshotsRequest newRequest() {
        return new GetSnapshotsRequest();
    }

    @Override
    protected GetSnapshotsResponse newResponse() {
        return new GetSnapshotsResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotsRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
    }

    @Override
    protected void masterOperation(final GetSnapshotsRequest request, ClusterState state, final ActionListener<GetSnapshotsResponse> listener) throws ElasticsearchException {
        SnapshotId[] snapshotIds = new SnapshotId[request.snapshots().length];
        for (int i = 0; i < snapshotIds.length; i++) {
            snapshotIds[i] = new SnapshotId(request.repository(), request.snapshots()[i]);
        }

        try {
            ImmutableList.Builder<SnapshotInfo> snapshotInfoBuilder = ImmutableList.builder();
            if (snapshotIds.length > 0) {
                for (SnapshotId snapshotId : snapshotIds) {
                    snapshotInfoBuilder.add(new SnapshotInfo(snapshotsService.snapshot(snapshotId)));
                }
            } else {
                ImmutableList<Snapshot> snapshots = snapshotsService.snapshots(request.repository());
                for (Snapshot snapshot : snapshots) {
                    snapshotInfoBuilder.add(new SnapshotInfo(snapshot));
                }
            }
            listener.onResponse(new GetSnapshotsResponse(snapshotInfoBuilder.build()));
        } catch (Throwable t) {
            listener.onFailure(t);
        }
    }
}
