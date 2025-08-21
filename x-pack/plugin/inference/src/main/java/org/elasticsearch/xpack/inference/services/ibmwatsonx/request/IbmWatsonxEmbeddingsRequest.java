/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class IbmWatsonxEmbeddingsRequest implements IbmWatsonxRequest {

    private final Truncator truncator;
    private final Truncator.TruncationResult truncationResult;
    private final IbmWatsonxEmbeddingsModel model;

    public IbmWatsonxEmbeddingsRequest(Truncator truncator, Truncator.TruncationResult input, IbmWatsonxEmbeddingsModel model) {
        this.truncator = Objects.requireNonNull(truncator);
        this.truncationResult = Objects.requireNonNull(input);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(model.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(
                new IbmWatsonxEmbeddingsRequestEntity(
                    truncationResult.input(),
                    model.getServiceSettings().modelId(),
                    model.getServiceSettings().projectId()
                )
            ).getBytes(StandardCharsets.UTF_8)
        );

        httpPost.setEntity(byteEntity);
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());

        decorateWithAuth(httpPost);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    public void decorateWithAuth(HttpPost httpPost) {
        IbmWatsonxRequest.decorateWithBearerToken(httpPost, model.getSecretSettings(), model.getInferenceEntityId());
    }

    public Truncator truncator() {
        return truncator;
    }

    public Truncator.TruncationResult truncationResult() {
        return truncationResult;
    }

    public IbmWatsonxEmbeddingsModel model() {
        return model;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public URI getURI() {
        return model.uri();
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());

        return new IbmWatsonxEmbeddingsRequest(truncator, truncatedInput, model);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }
}
