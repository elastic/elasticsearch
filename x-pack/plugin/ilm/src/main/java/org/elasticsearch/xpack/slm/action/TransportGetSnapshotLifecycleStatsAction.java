/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleStatsAction;
import org.elasticsearch.xpack.slm.SnapshotLifecycleStats;

import java.io.IOException;

public class TransportGetSnapshotLifecycleStatsAction extends
    TransportMasterNodeAction<GetSnapshotLifecycleStatsAction.Request, GetSnapshotLifecycleStatsAction.Response> {

    @Inject
    public TransportGetSnapshotLifecycleStatsAction(TransportService transportService, ClusterService clusterService,
                                                    ThreadPool threadPool, ActionFilters actionFilters,
                                                    IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetSnapshotLifecycleStatsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetSnapshotLifecycleStatsAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetSnapshotLifecycleStatsAction.Response read(StreamInput in) throws IOException {
        return new GetSnapshotLifecycleStatsAction.Response(in);
    }

    @Override
    protected void masterOperation(Task task, GetSnapshotLifecycleStatsAction.Request request,
                                   ClusterState state, ActionListener<GetSnapshotLifecycleStatsAction.Response> listener) {
        SnapshotLifecycleMetadata slmMeta = state.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        if (slmMeta == null) {
            listener.onResponse(new GetSnapshotLifecycleStatsAction.Response(new SnapshotLifecycleStats()));
        } else {
            listener.onResponse(new GetSnapshotLifecycleStatsAction.Response(slmMeta.getStats()));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotLifecycleStatsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
