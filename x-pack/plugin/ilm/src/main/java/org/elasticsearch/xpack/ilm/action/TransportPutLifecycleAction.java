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
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.ilm.PutLifecycleMetadataService;

import java.util.Optional;
import java.util.Set;

/**
 * This class is responsible for bootstrapping {@link IndexLifecycleMetadata} into the cluster-state, as well
 * as adding the desired new policy to be inserted.
 */
public class TransportPutLifecycleAction extends TransportMasterNodeAction<PutLifecycleRequest, AcknowledgedResponse> {

    private final PutLifecycleMetadataService putLifecycleMetadataService;

    @Inject
    public TransportPutLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        PutLifecycleMetadataService putLifecycleMetadataService
    ) {
        super(
            ILMActions.PUT.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutLifecycleRequest::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.putLifecycleMetadataService = putLifecycleMetadataService;
    }

    @Override
    protected void masterOperation(
        Task task,
        PutLifecycleRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        putLifecycleMetadataService.addLifecycle(request, state, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PutLifecycleRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedLifecycleAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(PutLifecycleRequest request) {
        return Set.of(request.getPolicy().getName());
    }
}
