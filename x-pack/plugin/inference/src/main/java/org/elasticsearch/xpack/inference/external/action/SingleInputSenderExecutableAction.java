/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.RequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;

import java.util.Objects;

public class SingleInputSenderExecutableAction extends SenderExecutableAction {
    private final String requestTypeForInputValidationError;

    public SingleInputSenderExecutableAction(
        Sender sender,
        RequestManager requestManager,
        String failedToSendRequestErrorMessage,
        String requestTypeForInputValidationError
    ) {
        super(sender, requestManager, failedToSendRequestErrorMessage);
        this.requestTypeForInputValidationError = Objects.requireNonNull(requestTypeForInputValidationError);
    }

    @Override
    public void execute(InferenceInputs inferenceInputs, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        if (inferenceInputs instanceof DocumentsOnlyInput == false) {
            listener.onFailure(new ElasticsearchStatusException("Invalid inference input type", RestStatus.INTERNAL_SERVER_ERROR));
            return;
        }

        var docsOnlyInput = (DocumentsOnlyInput) inferenceInputs;
        if (docsOnlyInput.getInputs().size() > 1) {
            listener.onFailure(
                new ElasticsearchStatusException(requestTypeForInputValidationError + " only accepts 1 input", RestStatus.BAD_REQUEST)
            );
            return;
        }

        super.execute(inferenceInputs, timeout, listener);
    }

}
