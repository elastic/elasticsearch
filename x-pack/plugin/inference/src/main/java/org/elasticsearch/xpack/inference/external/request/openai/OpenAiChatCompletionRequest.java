/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.external.request.openai.OpenAiUtils.createOrgHeader;

public class OpenAiChatCompletionRequest implements OpenAiRequest {

    private final OpenAiAccount account;
    private final List<String> input;
    private final URI uri;
    private final OpenAiChatCompletionModel model;

    public OpenAiChatCompletionRequest(OpenAiAccount account, List<String> input, OpenAiChatCompletionModel model) {
        this.account = Objects.requireNonNull(account);
        this.input = Objects.requireNonNull(input);
        this.uri = buildUri(this.account.url(), "OpenAI", OpenAiChatCompletionRequest::buildDefaultUri);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(
                new OpenAiChatCompletionRequestEntity(input, model.getServiceSettings().modelId(), model.getTaskSettings().user())
            ).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(createAuthBearerHeader(account.apiKey()));

        var org = account.organizationId();
        if (org != null) {
            httpPost.setHeader(createOrgHeader(org));
        }

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public Request truncate() {
        // No truncation for OpenAI chat completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for OpenAI chat completions
        return null;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    // default for testing
    static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(OpenAiUtils.HOST)
            .setPathSegments(OpenAiUtils.VERSION_1, OpenAiUtils.CHAT_PATH, OpenAiUtils.COMPLETIONS_PATH)
            .build();
    }
}
