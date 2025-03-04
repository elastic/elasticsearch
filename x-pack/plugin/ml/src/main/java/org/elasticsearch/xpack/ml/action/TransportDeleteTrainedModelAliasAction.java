/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAliasAction;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.core.ml.utils.InferenceProcessorInfoExtractor;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TransportDeleteTrainedModelAliasAction extends AcknowledgedTransportMasterNodeAction<DeleteTrainedModelAliasAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteTrainedModelAliasAction.class);

    private final InferenceAuditor auditor;
    private final IngestService ingestService;

    @Inject
    public TransportDeleteTrainedModelAliasAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        InferenceAuditor auditor,
        IngestService ingestService
    ) {
        super(
            DeleteTrainedModelAliasAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteTrainedModelAliasAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.auditor = auditor;
        this.ingestService = ingestService;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteTrainedModelAliasAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        submitUnbatchedTask("delete-model-alias", new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(final ClusterState currentState) {
                return deleteModelAlias(currentState, ingestService, auditor, request);
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    static ClusterState deleteModelAlias(
        final ClusterState currentState,
        final IngestService ingestService,
        final InferenceAuditor inferenceAuditor,
        final DeleteTrainedModelAliasAction.Request request
    ) {
        final ModelAliasMetadata currentMetadata = ModelAliasMetadata.fromState(currentState);
        final String referencedModel = currentMetadata.getModelId(request.getModelAlias());
        if (referencedModel == null) {
            throw new ElasticsearchStatusException("model_alias [{}] could not be found", RestStatus.NOT_FOUND, request.getModelAlias());
        }
        if (referencedModel.equals(request.getModelId()) == false) {
            throw new ElasticsearchStatusException(
                "model_alias [{}] does not refer to provided model_id [{}]",
                RestStatus.CONFLICT,
                request.getModelAlias(),
                request.getModelId()
            );
        }
        IngestMetadata currentIngestMetadata = currentState.metadata().getProject().custom(IngestMetadata.TYPE);
        Set<String> referencedModels = InferenceProcessorInfoExtractor.getModelIdsFromInferenceProcessors(currentIngestMetadata);
        if (referencedModels.contains(request.getModelAlias())) {
            throw new ElasticsearchStatusException(
                "Cannot delete model_alias [{}] as it is still referenced by ingest processors",
                RestStatus.CONFLICT,
                request.getModelAlias()
            );
        }
        final ClusterState.Builder builder = ClusterState.builder(currentState);
        final Map<String, ModelAliasMetadata.ModelAliasEntry> newMetadata = new HashMap<>(currentMetadata.modelAliases());
        logger.info("deleting model_alias [{}] that refers to model [{}]", request.getModelAlias(), request.getModelId());
        inferenceAuditor.info(referencedModel, String.format(Locale.ROOT, "deleting model_alias [%s]", request.getModelAlias()));

        newMetadata.remove(request.getModelAlias());
        final ModelAliasMetadata modelAliasMetadata = new ModelAliasMetadata(newMetadata);
        builder.metadata(Metadata.builder(currentState.getMetadata()).putCustom(ModelAliasMetadata.NAME, modelAliasMetadata).build());
        return builder.build();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteTrainedModelAliasAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
