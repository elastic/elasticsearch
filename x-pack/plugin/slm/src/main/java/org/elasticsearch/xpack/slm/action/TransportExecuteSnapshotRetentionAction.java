/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotRetentionAction;
import org.elasticsearch.xpack.slm.SnapshotRetentionService;

public class TransportExecuteSnapshotRetentionAction extends AcknowledgedTransportMasterNodeAction<ExecuteSnapshotRetentionAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportExecuteSnapshotRetentionAction.class);

    private final SnapshotRetentionService retentionService;

    @Inject
    public TransportExecuteSnapshotRetentionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        SnapshotRetentionService retentionService
    ) {
        super(
            ExecuteSnapshotRetentionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ExecuteSnapshotRetentionAction.Request::new,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
        this.retentionService = retentionService;
    }

    @Override
    protected void masterOperation(
        final Task task,
        final ExecuteSnapshotRetentionAction.Request request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        try {
            logger.info("manually triggering SLM snapshot retention");
            this.retentionService.triggerRetention();
            listener.onResponse(AcknowledgedResponse.TRUE);
        } catch (Exception e) {
            listener.onFailure(new ElasticsearchException("failed to execute snapshot lifecycle retention", e));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(ExecuteSnapshotRetentionAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
