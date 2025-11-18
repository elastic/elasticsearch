/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.FlushTrainedModelCacheAction;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelCacheMetadataService;

public class TransportFlushTrainedModelCacheAction extends AcknowledgedTransportMasterNodeAction<FlushTrainedModelCacheAction.Request> {

    private final TrainedModelCacheMetadataService modelCacheMetadataService;

    @Inject
    public TransportFlushTrainedModelCacheAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        TrainedModelCacheMetadataService modelCacheMetadataService
    ) {
        super(
            FlushTrainedModelCacheAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            FlushTrainedModelCacheAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.modelCacheMetadataService = modelCacheMetadataService;
    }

    @Override
    protected void masterOperation(
        Task task,
        FlushTrainedModelCacheAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        modelCacheMetadataService.updateCacheVersion(listener);
    }

    @Override
    protected ClusterBlockException checkBlock(FlushTrainedModelCacheAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
