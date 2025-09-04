/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchAccount;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchSparseRequestManager;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModel;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class AlibabaCloudSearchSparseAction implements ExecutableAction {
    private static final Logger logger = LogManager.getLogger(AlibabaCloudSearchSparseAction.class);

    private final AlibabaCloudSearchAccount account;
    private final AlibabaCloudSearchSparseModel model;
    private final String failedToSendRequestErrorMessage;
    private final Sender sender;
    private final AlibabaCloudSearchSparseRequestManager requestCreator;

    public AlibabaCloudSearchSparseAction(Sender sender, AlibabaCloudSearchSparseModel model, ServiceComponents serviceComponents) {
        this.model = Objects.requireNonNull(model);
        this.account = new AlibabaCloudSearchAccount(this.model.getSecretSettings().apiKey());
        this.failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("AlibabaCloud Search sparse embeddings");
        this.sender = Objects.requireNonNull(sender);
        requestCreator = AlibabaCloudSearchSparseRequestManager.of(account, model, serviceComponents.threadPool());
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
