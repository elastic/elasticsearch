/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.action;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchAccount;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModel;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class AlibabaCloudSearchEmbeddingsAction implements ExecutableAction {
    private final AlibabaCloudSearchAccount account;
    private final AlibabaCloudSearchEmbeddingsModel model;
    private final String failedToSendRequestErrorMessage;
    private final Sender sender;
    private final AlibabaCloudSearchEmbeddingsRequestManager requestCreator;

    public AlibabaCloudSearchEmbeddingsAction(Sender sender, AlibabaCloudSearchEmbeddingsModel model, ServiceComponents serviceComponents) {
        this.model = Objects.requireNonNull(model);
        this.sender = Objects.requireNonNull(sender);
        this.account = new AlibabaCloudSearchAccount(this.model.getSecretSettings().apiKey());
        this.failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("AlibabaCloud Search text embeddings");
        this.requestCreator = AlibabaCloudSearchEmbeddingsRequestManager.of(account, model, serviceComponents.threadPool());
    }

    @Override
    public void execute(InferenceInputs inferenceInputs, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        try {
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(
                failedToSendRequestErrorMessage,
                listener
            );
            sender.send(requestCreator, inferenceInputs, timeout, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, failedToSendRequestErrorMessage));
        }
    }
}
