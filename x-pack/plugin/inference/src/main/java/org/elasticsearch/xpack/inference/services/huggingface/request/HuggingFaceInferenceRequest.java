/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class HuggingFaceInferenceRequest implements Request {

    private final Truncator truncator;
    private final HuggingFaceAccount account;
    private final Truncator.TruncationResult truncationResult;
    private final HuggingFaceModel model;

    public HuggingFaceInferenceRequest(Truncator truncator, Truncator.TruncationResult input, HuggingFaceModel model) {
        this.truncator = Objects.requireNonNull(truncator);
        this.account = HuggingFaceAccount.of(model);
        this.truncationResult = Objects.requireNonNull(input);
        this.model = Objects.requireNonNull(model);
    }

    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(account.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new HuggingFaceInferenceRequestEntity(truncationResult.input())).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());
        httpPost.setHeader(createAuthBearerHeader(account.apiKey()));

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    public URI getURI() {
        return account.uri();
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public Request truncate() {
        var truncateResult = truncator.truncate(truncationResult.input());

        return new HuggingFaceInferenceRequest(truncator, truncateResult, model);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }
}
