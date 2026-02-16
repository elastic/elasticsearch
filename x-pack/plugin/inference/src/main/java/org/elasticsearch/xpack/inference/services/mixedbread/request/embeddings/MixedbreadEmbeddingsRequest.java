/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.request.embeddings;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.mixedbread.embeddings.MixedbreadEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

/**
 * Mixedbread Embeddings Request
 * This class is responsible for creating a request to the Mixedbread embeddings endpoint.
 * It constructs an HTTP POST request with the necessary headers and body content.
 */
public class MixedbreadEmbeddingsRequest implements Request {
    private final MixedbreadEmbeddingsModel model;
    private final Truncator.TruncationResult truncationResult;
    private final Truncator truncator;

    /**
     * Constructs a new {@link MixedbreadEmbeddingsRequest} with the specified truncator, input, and model.
     *
     * @param truncator the truncator to handle input truncation
     * @param input the input to be truncated
     * @param model the Mixedbread embeddings model to be used for the request
     */
    public MixedbreadEmbeddingsRequest(Truncator truncator, Truncator.TruncationResult input, MixedbreadEmbeddingsModel model) {
        this.model = Objects.requireNonNull(model);
        this.truncator = truncator;
        this.truncationResult = input;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(getURI());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(
                new MixedbreadEmbeddingsRequestEntity(
                    truncationResult.input(),
                    model.getServiceSettings().modelId(),
                    model.getServiceSettings().dimensions(),
                    model.getTaskSettings().getPrompt(),
                    model.getTaskSettings().getNormalized(),
                    model.getServiceSettings().encodingFormat(),
                    model.getServiceSettings().dimensionsSetByUser()
                )
            ).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(createAuthBearerHeader(model.getSecretSettings().apiKey()));

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return model.uri();
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
        return model.getInferenceEntityId();
    }
}
