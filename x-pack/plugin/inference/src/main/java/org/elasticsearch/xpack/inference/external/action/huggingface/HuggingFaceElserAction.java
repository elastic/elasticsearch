/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.huggingface;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceClient;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequest;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequestEntity;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;

import java.util.List;

import static org.elasticsearch.core.Strings.format;

public class HuggingFaceElserAction implements ExecutableAction {

    private final HuggingFaceAccount account;
    private final HuggingFaceClient client;

    public HuggingFaceElserAction(Sender sender, HuggingFaceElserModel model, ServiceComponents serviceComponents) {
        this.client = new HuggingFaceClient(sender, serviceComponents);
        this.account = new HuggingFaceAccount(model.getServiceSettings().uri(), model.getSecretSettings().apiKey());
    }

    @Override
    public void execute(List<String> input, ActionListener<List<? extends InferenceResults>> listener) {
        try {
            HuggingFaceElserRequest request = new HuggingFaceElserRequest(account, new HuggingFaceElserRequestEntity(input));

            ActionListener<List<? extends InferenceResults>> wrapFailuresInElasticsearchExceptionListener = ActionListener.wrap(
                listener::onResponse,
                e -> {
                    var unwrappedException = ExceptionsHelper.unwrapCause(e);

                    if (unwrappedException instanceof ElasticsearchException esException) {
                        listener.onFailure(esException);
                    } else {
                        listener.onFailure(createInternalServerError(unwrappedException));
                    }
                }
            );

            client.send(request, wrapFailuresInElasticsearchExceptionListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e));
        }
    }

    private ElasticsearchStatusException createInternalServerError(Throwable e) {
        return new ElasticsearchStatusException(
            format("Failed to send ELSER Hugging Face request to [%s]", account.url()),
            RestStatus.INTERNAL_SERVER_ERROR,
            e
        );
    }
}
