/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.mixedbread.embeddings.MixedbreadEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class MixedbreadEmbeddingsRequest implements Request {
    private final URI uri;
    private final MixedbreadEmbeddingsModel model;
    private final String inferenceEntityId;
    private final Truncator.TruncationResult truncationResult;
    private final Truncator truncator;

    /**
     * Constructs a new MixedbreadEmbeddingsRequest with the specified truncator, input, and model.
     *
     * @param truncator the truncator to handle input truncation
     * @param input the input to be truncated
     * @param model the Mixedbread embeddings model to be used for the request
     */
    public MixedbreadEmbeddingsRequest(Truncator truncator, Truncator.TruncationResult input, MixedbreadEmbeddingsModel model) {
        this.uri = model.uri();
        this.model = model;
        this.inferenceEntityId = model.getInferenceEntityId();
        this.truncator = truncator;
        this.truncationResult = input;
    }

    /**
     * Returns the URI for this request.
     *
     * @return the URI of the Mixedbread embeddings model
     */
    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(this.uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new MixedbreadEmbeddingsRequestEntity(model.model(), truncationResult.input()))
                .getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());
        if (model.getSecretSettings() instanceof DefaultSecretSettings) {
            var secretSettings = model.getSecretSettings();
            httpPost.setHeader(createAuthBearerHeader(secretSettings.apiKey()));
        }

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());
        return new MixedbreadEmbeddingsRequest(truncator, truncatedInput, model);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }

    @Override
    public String getInferenceEntityId() {
        return inferenceEntityId;
    }
}
