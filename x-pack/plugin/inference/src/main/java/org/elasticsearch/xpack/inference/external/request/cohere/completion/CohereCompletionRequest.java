/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere.completion;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.request.cohere.CohereRequest;
import org.elasticsearch.xpack.inference.external.request.cohere.CohereUtils;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionModel;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class CohereCompletionRequest extends CohereRequest {
    private final CohereAccount account;
    private final List<String> input;
    private final String modelId;
    private final String inferenceEntityId;
    private final boolean stream;

    public CohereCompletionRequest(List<String> input, CohereCompletionModel model, boolean stream) {
        Objects.requireNonNull(model);

        this.account = CohereAccount.of(model, CohereCompletionRequest::buildDefaultUri);
        this.input = Objects.requireNonNull(input);
        this.modelId = model.getServiceSettings().modelId();
        this.inferenceEntityId = model.getInferenceEntityId();
        this.stream = stream;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(account.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new CohereCompletionRequestEntity(input, modelId, isStreaming())).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        decorateWithAuthHeader(httpPost, account);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public String getInferenceEntityId() {
        return inferenceEntityId;
    }

    @Override
    public boolean isStreaming() {
        return stream;
    }

    @Override
    public URI getURI() {
        return account.uri();
    }

    @Override
    public Request truncate() {
        // no truncation
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // no truncation
        return null;
    }

    public static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(CohereUtils.HOST)
            .setPathSegments(CohereUtils.VERSION_1, CohereUtils.CHAT_PATH)
            .build();
    }
}
