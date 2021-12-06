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
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction.Request;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelLocation;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportPutTrainedModelDefinitionPartAction extends HandledTransportAction<Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutTrainedModelDefinitionPartAction.class);
    private final TrainedModelProvider trainedModelProvider;
    private final XPackLicenseState licenseState;
    private final OriginSettingClient client;

    @Inject
    public TransportPutTrainedModelDefinitionPartAction(
        TransportService transportService,
        XPackLicenseState licenseState,
        ActionFilters actionFilters,
        Client client,
        TrainedModelProvider trainedModelProvider
    ) {
        super(
            PutTrainedModelDefinitionPartAction.NAME,
            transportService,
            actionFilters,
            Request::new
        );
        this.licenseState = licenseState;
        this.trainedModelProvider = trainedModelProvider;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<AcknowledgedResponse> listener) {
        if (MachineLearningField.ML_API_FEATURE.check(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

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
                    // XContentParser::binaryValue pulls out the raw, base64 decoded bytes automatically. So, we only need the length here
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
}
