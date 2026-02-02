/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action to mark an index to be force merged by updating its custom metadata.
 */
public class TransportMarkIndexToBeForceMergedAction extends TransportMasterNodeAction<
    MarkIndexToBeForceMergedAction.Request,
    AcknowledgedResponse> {

    private final MarkIndexToBeForceMergedAction.TaskQueueManager taskQueueManager;

    @Inject
    public TransportMarkIndexToBeForceMergedAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            MarkIndexToBeForceMergedAction.INSTANCE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            MarkIndexToBeForceMergedAction.Request::new,
            AcknowledgedResponse::readFrom,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.taskQueueManager = new MarkIndexToBeForceMergedAction.TaskQueueManager(clusterService);
    }

    @Override
    protected void masterOperation(
        Task task,
        MarkIndexToBeForceMergedAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueueManager.submitTask(request.getSourceIndex(), request.getIndexToBeForceMerged(), request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(MarkIndexToBeForceMergedAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
