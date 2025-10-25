/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

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
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction.Request;
import org.elasticsearch.xpack.ilm.LifecycleMetadataService;

import java.util.Optional;
import java.util.Set;

public class TransportDeleteLifecycleAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private final LifecycleMetadataService lifecycleMetadataService;

    @Inject
    public TransportDeleteLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        LifecycleMetadataService lifecycleMetadataService
    ) {
        super(
            DeleteLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.lifecycleMetadataService = lifecycleMetadataService;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        lifecycleMetadataService.deleteLifecycle(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedLifecycleAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(Request request) {
        return Set.of(request.getPolicyName());
    }
}
