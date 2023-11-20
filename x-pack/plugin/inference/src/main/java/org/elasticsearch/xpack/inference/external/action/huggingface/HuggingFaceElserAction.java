/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.huggingface;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceResults;
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
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class HuggingFaceElserAction implements ExecutableAction {

    private final HuggingFaceAccount account;
    private final HuggingFaceClient client;
    private final String errorMessage;

    public HuggingFaceElserAction(Sender sender, HuggingFaceElserModel model, ServiceComponents serviceComponents) {
        this.client = new HuggingFaceClient(sender, serviceComponents);
        this.account = new HuggingFaceAccount(model.getServiceSettings().uri(), model.getSecretSettings().apiKey());
        this.errorMessage = format("Failed to send ELSER Hugging Face request to [%s]", model.getServiceSettings().uri().toString());
    }

    @Override
    public void execute(List<String> input, ActionListener<List<? extends InferenceResults>> listener) {
        try {
            HuggingFaceElserRequest request = new HuggingFaceElserRequest(account, new HuggingFaceElserRequestEntity(input));
            ActionListener<List<? extends InferenceResults>> wrappedListener = wrapFailuresInElasticsearchException(errorMessage, listener);

            client.send(request, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, errorMessage));
        }
    }
}
