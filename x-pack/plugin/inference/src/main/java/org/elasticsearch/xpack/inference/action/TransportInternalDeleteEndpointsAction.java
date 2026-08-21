/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InternalDeleteInferenceEndpointsAction;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.Objects;

/**
 * Handles the internal action for deleting multiple inference endpoints. This should not be used by external REST APIs.
 * This implementation differs from {@link TransportDeleteInferenceEndpointAction} in that this one can handle deleting multiple endpoints
 * at a time, but it does not perform the safeguard checks. This implementation will not check to see if the default endpoint is
 * referenced by pipelines or indices.
 */
public class TransportInternalDeleteEndpointsAction extends TransportMasterNodeAction<
    InternalDeleteInferenceEndpointsAction.Request,
    AcknowledgedResponse> {

    private final ModelRegistry modelRegistry;

    @Inject
    public TransportInternalDeleteEndpointsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ModelRegistry modelRegistry
    ) {
        super(
            InternalDeleteInferenceEndpointsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            InternalDeleteInferenceEndpointsAction.Request::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.modelRegistry = Objects.requireNonNull(modelRegistry);
    }

    @Override
    protected void masterOperation(
        Task task,
        InternalDeleteInferenceEndpointsAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> masterListener
    ) {
        modelRegistry.deleteModels(
            request.getInferenceEntityIds(),
            masterListener.delegateFailureAndWrap((l, r) -> l.onResponse(AcknowledgedResponse.TRUE))
        );
    }

    @Override
    protected ClusterBlockException checkBlock(InternalDeleteInferenceEndpointsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
