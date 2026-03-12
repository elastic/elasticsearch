/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;

import java.net.URI;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.services.azureaistudio.request.AzureAiStudioRequestFields.API_KEY_HEADER;

public abstract class AzureAiStudioRequest implements Request {

    protected final URI uri;
    protected final String inferenceEntityId;

    protected final boolean isOpenAiRequest;
    protected final boolean isRealtimeEndpoint;

    protected AzureAiStudioRequest(AzureAiStudioModel model) {
        this.uri = model.uri();
        this.inferenceEntityId = model.getInferenceEntityId();
        this.isOpenAiRequest = (model.provider() == AzureAiStudioProvider.OPENAI);
        this.isRealtimeEndpoint = (model.endpointType() == AzureAiStudioEndpointType.REALTIME);
    }

    protected void setAuthHeader(HttpEntityEnclosingRequestBase request, AzureAiStudioModel model) {
        var apiKey = model.getSecretSettings().apiKey();

        if (isOpenAiRequest) {
            request.setHeader(API_KEY_HEADER, apiKey.toString());
        } else {
            if (isRealtimeEndpoint) {
                request.setHeader(createAuthBearerHeader(apiKey));
            } else {
                request.setHeader(HttpHeaders.AUTHORIZATION, apiKey.toString());
            }
        }
    }

    @Override
    public URI getURI() {
        return this.uri;
    }

    @Override
    public String getInferenceEntityId() {
        return this.inferenceEntityId;
    }

}
