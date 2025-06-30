/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.request;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadModel;

import java.net.URI;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.services.azureaistudio.request.AzureAiStudioRequestFields.API_KEY_HEADER;

public abstract class MixedbreadRequest implements Request {
    protected final URI uri;
    protected final String inferenceEntityId;

    protected MixedbreadRequest(MixedbreadModel model) {
        this.uri = model.uri();
        this.inferenceEntityId = model.getInferenceEntityId();
    }

    protected void setAuthHeader(HttpEntityEnclosingRequestBase request, MixedbreadModel model) {
        var apiKey = model.getSecretSettings().apiKey();
        request.setHeader(API_KEY_HEADER, apiKey.toString());
        request.setHeader(createAuthBearerHeader(apiKey));
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
