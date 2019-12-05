/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotRetentionAction;
import org.elasticsearch.xpack.slm.SnapshotRetentionService;

import java.io.IOException;

public class TransportExecuteSnapshotRetentionAction
    extends TransportMasterNodeAction<ExecuteSnapshotRetentionAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportExecuteSnapshotRetentionAction.class);

    private final SnapshotRetentionService retentionService;

    @Inject
    public TransportExecuteSnapshotRetentionAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                                   SnapshotRetentionService retentionService) {
        super(ExecuteSnapshotRetentionAction.NAME, transportService, clusterService, threadPool, actionFilters,
            ExecuteSnapshotRetentionAction.Request::new, indexNameExpressionResolver);
        this.retentionService = retentionService;
    }
    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(final Task task, final ExecuteSnapshotRetentionAction.Request request,
                                   final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {
        try {
            logger.info("manually triggering SLM snapshot retention");
            this.retentionService.triggerRetention();
            listener.onResponse(new AcknowledgedResponse(true));
        } catch (Exception e) {
            listener.onFailure(new ElasticsearchException("failed to execute snapshot lifecycle retention", e));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(ExecuteSnapshotRetentionAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
