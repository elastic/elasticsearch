/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
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
import org.elasticsearch.xpack.core.inference.action.StoreInferenceEndpointsAction;
import org.elasticsearch.xpack.core.inference.results.ModelStoreResponse;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.List;
import java.util.Objects;

/**
 * Handles the internal action for creating multiple inference endpoints. This should not be used by external REST APIs.
 */
public class TransportStoreEndpointsAction extends TransportMasterNodeAction<
    StoreInferenceEndpointsAction.Request,
    StoreInferenceEndpointsAction.Response> {

    private final ModelRegistry modelRegistry;

    @Inject
    public TransportStoreEndpointsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ModelRegistry modelRegistry
    ) {
        super(
            StoreInferenceEndpointsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            StoreInferenceEndpointsAction.Request::new,
            StoreInferenceEndpointsAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.modelRegistry = Objects.requireNonNull(modelRegistry);
    }

    @Override
    protected void masterOperation(
        Task task,
        StoreInferenceEndpointsAction.Request request,
        ClusterState state,
        ActionListener<StoreInferenceEndpointsAction.Response> masterListener
    ) {
        SubscribableListener.<List<ModelStoreResponse>>newForked(
            listener -> modelRegistry.storeModels(request.getModels(), listener, request.masterNodeTimeout())
        ).andThenApply(StoreInferenceEndpointsAction.Response::new).addListener(masterListener);
    }

    @Override
    protected ClusterBlockException checkBlock(StoreInferenceEndpointsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
