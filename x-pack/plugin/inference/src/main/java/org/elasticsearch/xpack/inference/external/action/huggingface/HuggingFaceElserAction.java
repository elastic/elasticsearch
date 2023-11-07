/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.huggingface;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceClient;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequest;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequestEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;

public class HuggingFaceElserAction implements ExecutableAction {

    private final HuggingFaceAccount account;
    private final HuggingFaceClient client;

    public HuggingFaceElserAction(Sender sender, HuggingFaceElserModel model, ThrottlerManager throttlerManager) {
        this.client = new HuggingFaceClient(sender, throttlerManager);
        this.account = new HuggingFaceAccount(model.getServiceSettings().uri(), model.getSecretSettings().apiKey());
    }

    public void execute(String input, ActionListener<InferenceResults> listener) {
        try {
            HuggingFaceElserRequest request = new HuggingFaceElserRequest(account, new HuggingFaceElserRequestEntity(input));

            client.send(request, listener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(
                new ElasticsearchStatusException("Failed to send ELSER Hugging Face request", RestStatus.INTERNAL_SERVER_ERROR, e)
            );
        }
    }
}
