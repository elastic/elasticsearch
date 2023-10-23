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
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserSecretSettings;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserServiceSettings;

import java.net.URI;
import java.net.URISyntaxException;

import static org.elasticsearch.core.Strings.format;

public class HuggingFaceElserAction implements ExecutableAction {
    private final String input;
    private final Sender sender;
    private final HuggingFaceElserServiceSettings serviceSettings;
    private final HuggingFaceElserSecretSettings secretSettings;

    public HuggingFaceElserAction(
        String input,
        Sender sender,
        HuggingFaceElserServiceSettings serviceSettings,
        HuggingFaceElserSecretSettings secretSettings
    ) {
        this.input = input;
        this.sender = sender;
        this.serviceSettings = serviceSettings;
        this.secretSettings = secretSettings;
    }

    public void execute(ActionListener<InferenceResults> listener) {
        try {
            URI url = new URI(serviceSettings.url());
            HuggingFaceAccount account = new HuggingFaceAccount(url, secretSettings.apiKey());
            HuggingFaceElserRequestEntity entity = new HuggingFaceElserRequestEntity(input);
            HuggingFaceElserRequest request = new HuggingFaceElserRequest(account, entity);
            HuggingFaceClient client = new HuggingFaceClient(sender);

            client.send(request, listener);
        } catch (URISyntaxException e) {
            listener.onFailure(
                new ElasticsearchException(format("Failed to parse url [%s] from service settings", serviceSettings.url()), e)
            );
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(
                new ElasticsearchStatusException("Failed to send request ELSER Hugging Face request", RestStatus.INTERNAL_SERVER_ERROR, e)
            );
        }
    }
}
