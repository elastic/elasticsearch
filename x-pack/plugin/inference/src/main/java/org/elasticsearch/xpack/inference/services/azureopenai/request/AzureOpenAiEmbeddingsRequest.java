/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class AzureOpenAiEmbeddingsRequest implements Request {

    private final Truncator truncator;
    private final Truncator.TruncationResult truncationResult;
    private final InputType inputType;
    private final URI uri;
    private final AzureOpenAiEmbeddingsModel model;

    public AzureOpenAiEmbeddingsRequest(
        Truncator truncator,
        Truncator.TruncationResult input,
        InputType inputType,
        AzureOpenAiEmbeddingsModel model
    ) {
        this.truncator = Objects.requireNonNull(truncator);
        this.truncationResult = Objects.requireNonNull(input);
        this.inputType = inputType;
        this.model = Objects.requireNonNull(model);
        this.uri = model.getUri();
    }

    @Override
    public HttpRequest createHttpRequest() {
        throw new UnsupportedOperationException("use createHttpRequestAsync() instead");
    }

    @Override
    public void createHttpRequestAsync(ActionListener<HttpRequest> listener) {
        HttpPost httpPost = new HttpPost(uri);

        String requestEntity = Strings.toString(
            new AzureOpenAiEmbeddingsRequestEntity(
                truncationResult.input(),
                inputType,
                model.getTaskSettings().user(),
                model.getServiceSettings().dimensions(),
                model.getServiceSettings().dimensionsSetByUser()
            )
        );

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestEntity.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(new BasicHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType()));
        model.getSecretSettings()
            .applyTo(
                httpPost,
                listener.delegateFailureAndWrap(
                    (httpRequestActionListener, httpRequestBase) -> new HttpRequest(httpRequestBase, getInferenceEntityId())
                )
            );
    }

    @Override
    public URI getURI() {
        return this.uri;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());

        return new AzureOpenAiEmbeddingsRequest(truncator, truncatedInput, inputType, model);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }
}
