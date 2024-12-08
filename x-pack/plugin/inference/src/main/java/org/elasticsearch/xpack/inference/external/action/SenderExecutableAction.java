/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.RequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class SenderExecutableAction implements ExecutableAction {

    private final Sender sender;
    private final RequestManager requestManager;
    private final String failedToSendRequestErrorMessage;

    public SenderExecutableAction(Sender sender, RequestManager requestManager, String failedToSendRequestErrorMessage) {
        this.sender = Objects.requireNonNull(sender);
        this.requestManager = Objects.requireNonNull(requestManager);
        this.failedToSendRequestErrorMessage = Objects.requireNonNull(failedToSendRequestErrorMessage);
    }

    @Override
    public void execute(InferenceInputs inferenceInputs, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        var wrappedListener = wrapFailuresInElasticsearchException(failedToSendRequestErrorMessage, listener);
        try {
            sender.send(requestManager, inferenceInputs, timeout, wrappedListener);
        } catch (Exception e) {
            wrappedListener.onFailure(e);
        }
    }
}
