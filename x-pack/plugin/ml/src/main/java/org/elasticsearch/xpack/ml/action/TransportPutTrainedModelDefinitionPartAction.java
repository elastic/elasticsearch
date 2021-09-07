/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction.Request;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelLocation;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportPutTrainedModelDefinitionPartAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutTrainedModelDefinitionPartAction.class);
    private final TrainedModelProvider trainedModelProvider;
    private final XPackLicenseState licenseState;
    private final OriginSettingClient client;

    @Inject
    public TransportPutTrainedModelDefinitionPartAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        TrainedModelProvider trainedModelProvider
    ) {
        super(
            PutTrainedModelDefinitionPartAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            AcknowledgedResponse::readFrom,
            ThreadPool.Names.SAME
        );
        this.licenseState = licenseState;
        this.trainedModelProvider = trainedModelProvider;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {

        ActionListener<TrainedModelConfig> configActionListener = ActionListener.wrap(config -> {
            TrainedModelLocation location = config.getLocation();
            if (location == null) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "cannot put definition for model [{}] as location is null",
                        RestStatus.BAD_REQUEST,
                        request.getModelId()
                    )
                );
                return;
            }
            final boolean isEos = request.getPart() == request.getTotalParts() - 1;
            final String indexName = ((IndexLocation) location).getIndexName();
            trainedModelProvider.storeTrainedModelDefinitionDoc(
                new TrainedModelDefinitionDoc.Builder().setModelId(request.getModelId())
                    .setDocNum(request.getPart())
                    .setEos(isEos)
                    .setDefinitionLength(request.getDefinition().length())
                    .setTotalDefinitionLength(request.getTotalDefinitionLength())
                    .setCompressionVersion(TrainedModelConfig.CURRENT_DEFINITION_COMPRESSION_VERSION)
                    .setBinaryData(request.getDefinition())
                    .build(),
                indexName,
                ActionListener.wrap(stored -> {
                    if (isEos) {
                        client.admin()
                            .indices()
                            .prepareRefresh(indexName)
                            .execute(ActionListener.wrap(refreshed -> listener.onResponse(AcknowledgedResponse.TRUE), failure -> {
                                logger.warn(
                                    () -> new ParameterizedMessage("[{}] failed to refresh index [{}]", request.getModelId(), indexName),
                                    failure
                                );
                                listener.onResponse(AcknowledgedResponse.TRUE);
                            }));
                        return;
                    }
                    listener.onResponse(AcknowledgedResponse.TRUE);
                }, listener::onFailure)
            );
        }, listener::onFailure);

        trainedModelProvider.getTrainedModel(request.getModelId(), GetTrainedModelsAction.Includes.empty(), configActionListener);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        //TODO do we really need to do this???
        return null;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<AcknowledgedResponse> listener) {
        if (licenseState.checkFeature(XPackLicenseState.Feature.MACHINE_LEARNING)) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }
}
