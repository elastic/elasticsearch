/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundDenseEmbeddingRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.openai.OpenAiUtils.createOrgHeader;

public class OpenAiEmbeddingsRequest implements OutboundDenseEmbeddingRequest {

    private final Truncator truncator;
    private final Truncator.TruncationResult truncationResult;
    private final OpenAiEmbeddingsModel model;

    public OpenAiEmbeddingsRequest(Truncator truncator, Truncator.TruncationResult input, OpenAiEmbeddingsModel model) {
        this.truncator = Objects.requireNonNull(truncator);
        this.truncationResult = Objects.requireNonNull(input);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(model.uri()).build();

        httpPost.setBody(
            Strings.toString(
                new OpenAiEmbeddingsRequestEntity(
                    truncationResult.input(),
                    model.getServiceSettings().modelId(),
                    model.getTaskSettings().user(),
                    model.getServiceSettings().dimensions(),
                    model.getServiceSettings().dimensionsSetByUser()
                )
            ).getBytes(StandardCharsets.UTF_8),
            ContentType.APPLICATION_JSON
        );

        var org = model.rateLimitServiceSettings().organizationId();
        if (org != null) {
            httpPost.setHeader(createOrgHeader(org));
        }

        if (model.getTaskSettings().headers() != null) {
            for (var header : model.getTaskSettings().headers().entrySet()) {
                httpPost.setHeader(header.getKey(), header.getValue());
            }
        }

        model.secretsApplier()
            .applyTo(
                httpPost,
                listener.delegateFailureAndWrap(
                    (requestActionListener, req) -> requestActionListener.onResponse(new HttpRequest(req, getInferenceEntityId()))
                )
            );
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
    public OutboundRequest truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());

        return new OpenAiEmbeddingsRequest(truncator, truncatedInput, model);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }

    @Override
    public TaskType getTaskType() {
        return model.getTaskType();
    }

    @Override
    public void onAuthenticationFailure() {
        model.secretsApplier().onAuthenticationFailure();
    }
}
