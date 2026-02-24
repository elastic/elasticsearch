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
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action to mark an index to be force merged by updating its custom metadata.
 */
public class TransportMarkIndexForDLMForceMergeAction extends AcknowledgedTransportMasterNodeAction<
    MarkIndexForDLMForceMergeAction.Request> {

    private final DataStreamLifecycleService dataStreamLifecycleService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportMarkIndexForDLMForceMergeAction(
        TransportService transportService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        DataStreamLifecycleService dataStreamLifecycleService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            MarkIndexForDLMForceMergeAction.TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            MarkIndexForDLMForceMergeAction.Request::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.dataStreamLifecycleService = dataStreamLifecycleService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        MarkIndexForDLMForceMergeAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final var project = projectResolver.getProjectMetadata(state);
        dataStreamLifecycleService.markIndexForDlmForceMerge(project.id(), request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(MarkIndexForDLMForceMergeAction.Request request, ClusterState state) {
        ClusterBlockException globalBlock = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (globalBlock != null) {
            return globalBlock;
        }
        final var project = projectResolver.getProjectMetadata(state);
        return state.blocks().indexBlockedException(project.id(), ClusterBlockLevel.METADATA_WRITE, request.getOriginalIndex());
    }
}
