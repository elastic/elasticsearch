/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.elastic;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.ElasticInferenceServiceSparseEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsModel;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class ElasticInferenceServiceSparseEmbeddingsAction implements ExecutableAction {

    private final String errorMessage;

    private final ElasticInferenceServiceSparseEmbeddingsRequestManager requestManager;

    private final Sender sender;

    public ElasticInferenceServiceSparseEmbeddingsAction(
        Sender sender,
        ElasticInferenceServiceSparseEmbeddingsModel model,
        ServiceComponents serviceComponents
    ) {
        Objects.requireNonNull(serviceComponents);
        Objects.requireNonNull(model);
        this.sender = Objects.requireNonNull(sender);
        this.requestManager = new ElasticInferenceServiceSparseEmbeddingsRequestManager(model, serviceComponents);
        this.errorMessage = constructFailedToSendRequestMessage(model.uri(), "Elastic Inference Service sparse embeddings");
    }

    @Override
    public void execute(InferenceInputs inferenceInputs, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        try {
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(errorMessage, listener);

            sender.send(requestManager, inferenceInputs, timeout, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, errorMessage));
        }
    }
}
