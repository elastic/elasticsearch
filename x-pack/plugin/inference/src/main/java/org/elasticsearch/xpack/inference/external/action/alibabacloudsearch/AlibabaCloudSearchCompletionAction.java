/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.alibabacloudsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.alibabacloudsearch.AlibabaCloudSearchAccount;
import org.elasticsearch.xpack.inference.external.http.sender.AlibabaCloudSearchCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModel;

import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class AlibabaCloudSearchCompletionAction implements ExecutableAction {
    private static final Logger logger = LogManager.getLogger(AlibabaCloudSearchCompletionAction.class);

    private final AlibabaCloudSearchAccount account;
    private final AlibabaCloudSearchCompletionModel model;
    private final String failedToSendRequestErrorMessage;
    private final Sender sender;
    private final AlibabaCloudSearchCompletionRequestManager requestCreator;

    public AlibabaCloudSearchCompletionAction(Sender sender, AlibabaCloudSearchCompletionModel model, ServiceComponents serviceComponents) {
        this.model = Objects.requireNonNull(model);
        this.sender = Objects.requireNonNull(sender);
        this.account = new AlibabaCloudSearchAccount(this.model.getSecretSettings().apiKey());
        this.failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(null, "AlibabaCloud Search completion");
        this.requestCreator = AlibabaCloudSearchCompletionRequestManager.of(account, model, serviceComponents.threadPool());
    }

    @Override
    public void execute(InferenceInputs inferenceInputs, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        if (inferenceInputs instanceof DocumentsOnlyInput == false) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    format("Invalid inference input type, task type [%s] do not support Field [query]", TaskType.COMPLETION),
                    RestStatus.INTERNAL_SERVER_ERROR
                )
            );
            return;
        }

        var docsOnlyInput = (DocumentsOnlyInput) inferenceInputs;
        if (docsOnlyInput.getInputs().size() % 2 == 0) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Alibaba Completion's inputs must be an odd number. The last input is the current query, "
                        + "all preceding inputs are the completion history as pairs of user input and the assistant's response.",
                    RestStatus.BAD_REQUEST
                )
            );
            return;
        }

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
