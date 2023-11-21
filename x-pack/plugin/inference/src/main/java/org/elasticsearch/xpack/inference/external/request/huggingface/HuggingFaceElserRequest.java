/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.huggingface;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class HuggingFaceElserRequest implements Request {

    private final HuggingFaceAccount account;
    private final HuggingFaceElserRequestEntity entity;

    public HuggingFaceElserRequest(HuggingFaceAccount account, HuggingFaceElserRequestEntity entity) {
        this.account = Objects.requireNonNull(account);
        this.entity = Objects.requireNonNull(entity);
    }

    public HttpRequestBase createRequest() {
        HttpPost httpPost = new HttpPost(account.url());

        ByteArrayEntity byteEntity = new ByteArrayEntity(Strings.toString(entity).getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());
        httpPost.setHeader(createAuthBearerHeader(account.apiKey()));

        return httpPost;
    }
}
