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
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
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

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * The action that allows users to put parts of the model definition.
 *
 * The action is a {@link HandledTransportAction} as opposed to a {@link org.elasticsearch.action.support.master.TransportMasterNodeAction}.
 * This comes with pros and cons. The benefit is that when a model is imported it may spread over hundreds of documents with
 * each one being of considerable size. Thus, making this a {@link HandledTransportAction} avoids putting that load on the master node.
 * On the downsides, it is care is needed when it comes to adding new fields on those trained model definition docs. The action
 * could execute on a node that is on a newer version than the master node. This may mean the native model index does not have
 * the mappings required for newly added fields on later versions.
 */
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
        super(PutTrainedModelDefinitionPartAction.NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
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
                                logger.warn(() -> format("[%s] failed to refresh index [%s]", request.getModelId(), indexName), failure);
                                listener.onResponse(AcknowledgedResponse.TRUE);
                            }));
                        return;
                    }
                    listener.onResponse(AcknowledgedResponse.TRUE);
                }, listener::onFailure),
                request.isOverwritingAllowed()
            );
        }, listener::onFailure);

        trainedModelProvider.getTrainedModel(request.getModelId(), GetTrainedModelsAction.Includes.empty(), null, configActionListener);
    }
}
