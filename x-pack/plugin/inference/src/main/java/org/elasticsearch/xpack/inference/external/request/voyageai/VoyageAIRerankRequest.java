/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.voyageai;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.voyageai.VoyageAIAccount;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankModel;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankTaskSettings;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class VoyageAIRerankRequest extends VoyageAIRequest {

    private final VoyageAIAccount account;
    private final String query;
    private final List<String> input;
    private final VoyageAIRerankTaskSettings taskSettings;
    private final String model;
    private final String inferenceEntityId;

    public VoyageAIRerankRequest(String query, List<String> input, VoyageAIRerankModel model) {
        Objects.requireNonNull(model);

        this.account = VoyageAIAccount.of(model);
        this.input = Objects.requireNonNull(input);
        this.query = Objects.requireNonNull(query);
        taskSettings = model.getTaskSettings();
        this.model = model.getServiceSettings().modelId();
        inferenceEntityId = model.getInferenceEntityId();
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(account.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new VoyageAIRerankRequestEntity(query, input, taskSettings, model)).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        decorateWithHeaders(httpPost, account);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public String getInferenceEntityId() {
        return inferenceEntityId;
    }

    @Override
    public URI getURI() {
        return account.uri();
    }

    @Override
    public Request truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }
}
