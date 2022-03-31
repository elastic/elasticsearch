/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
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
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction.Request;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RobertaTokenization;
import org.elasticsearch.xpack.ml.inference.nlp.Vocabulary;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

public class TransportPutTrainedModelVocabularyAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private final TrainedModelProvider trainedModelProvider;
    private final XPackLicenseState licenseState;

    @Inject
    public TransportPutTrainedModelVocabularyAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TrainedModelProvider trainedModelProvider
    ) {
        super(
            PutTrainedModelVocabularyAction.NAME,
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
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {

        ActionListener<TrainedModelConfig> configActionListener = ActionListener.wrap(config -> {
            InferenceConfig inferenceConfig = config.getInferenceConfig();
            if (inferenceConfig instanceof NlpConfig nlpConfig) {
                if (nlpConfig.getTokenization() instanceof RobertaTokenization && request.getMerges().isEmpty()) {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            "cannot put vocabulary for model [{}] as tokenizer type [{}] requires [{}] to be provided and non-empty",
                            RestStatus.BAD_REQUEST,
                            request.getModelId(),
                            nlpConfig.getTokenization().getName(),
                            Request.MERGES.getPreferredName()
                        )
                    );
                    return;
                }
                trainedModelProvider.storeTrainedModelVocabulary(
                    request.getModelId(),
                    ((NlpConfig) inferenceConfig).getVocabularyConfig(),
                    new Vocabulary(request.getVocabulary(), request.getModelId(), request.getMerges()),
                    ActionListener.wrap(stored -> listener.onResponse(AcknowledgedResponse.TRUE), listener::onFailure)
                );
                return;
            }
            listener.onFailure(
                new ElasticsearchStatusException(
                    "cannot put vocabulary for model [{}] as it is not an NLP model",
                    RestStatus.BAD_REQUEST,
                    request.getModelId()
                )
            );
        }, listener::onFailure);

        trainedModelProvider.getTrainedModel(request.getModelId(), GetTrainedModelsAction.Includes.empty(), configActionListener);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        // TODO do we really need to do this???
        return null;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<AcknowledgedResponse> listener) {
        if (MachineLearningField.ML_API_FEATURE.check(licenseState)) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }
}
