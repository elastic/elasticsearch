/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.cohere;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.CohereRerankExecutableRequestCreator;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankModel;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class CohereRerankAction implements ExecutableAction {
    private final String failedToSendRequestErrorMessage;
    private final Sender sender;
    private final CohereRerankExecutableRequestCreator requestCreator;

    public CohereRerankAction(Sender sender, CohereRerankModel model) {
        Objects.requireNonNull(model);
        this.sender = Objects.requireNonNull(sender);
        this.failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(
            model.getServiceSettings().getCommonSettings().getUri(),
            "Cohere rerank"
        );
        requestCreator = new CohereRerankExecutableRequestCreator(model);
    }

    @Override
    public void execute(String query, List<String> input, ActionListener<InferenceServiceResults> listener) {
        try {
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(
                failedToSendRequestErrorMessage,
                listener
            );
            sender.send(requestCreator, query, input, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, failedToSendRequestErrorMessage));
        }
    }
}
