/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.embeddings;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

/**
 * Nvidia Embeddings Request
 * This class is responsible for creating a request to the Nvidia embeddings endpoint.
 * It constructs an HTTP POST request with the necessary headers and body content.
 */
public class NvidiaEmbeddingsRequest implements Request {
    private final NvidiaEmbeddingsModel model;
    private final Truncator.TruncationResult truncationResult;
    private final Truncator truncator;
    private final InputType inputType;

    /**
     * Constructs a new {@link NvidiaEmbeddingsRequest} with the specified truncator, input, and model.
     *
     * @param truncator the truncator to handle input truncation
     * @param input     the input to be truncated
     * @param model     the Nvidia embeddings model to be used for the request
     * @param inputType the type of input being processed
     */
    public NvidiaEmbeddingsRequest(
        Truncator truncator,
        Truncator.TruncationResult input,
        NvidiaEmbeddingsModel model,
        @Nullable InputType inputType
    ) {
        this.model = Objects.requireNonNull(model);
        this.truncator = Objects.requireNonNull(truncator);
        this.truncationResult = Objects.requireNonNull(input);
        this.inputType = inputType;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(getURI());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(
                new NvidiaEmbeddingsRequestEntity(
                    truncationResult.input(),
                    model.getServiceSettings().modelId(),
                    extractInputTypeToUse(),
                    model.getTaskSettings().getTruncation()
                )
            ).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());
        httpPost.setHeader(createAuthBearerHeader(model.getSecretSettings().apiKey()));

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    /**
     * Extracts the input type to be used for the request.
     * It first checks if the inputType field is specified, then checks the model's task settings.
     * If neither is specified, it defaults to {@link InputType#SEARCH}.
     *
     * @return the {@link InputType} to be used for the request
     */
    private InputType extractInputTypeToUse() {
        if (InputType.isSpecified(inputType)) {
            return inputType;
        } else if (InputType.isSpecified(model.getTaskSettings().getInputType())) {
            return model.getTaskSettings().getInputType();
        } else {
            return InputType.SEARCH;
        }
    }

    @Override
    public URI getURI() {
        return model.getServiceSettings().uri();
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());
        return new NvidiaEmbeddingsRequest(truncator, truncatedInput, model, inputType);
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
