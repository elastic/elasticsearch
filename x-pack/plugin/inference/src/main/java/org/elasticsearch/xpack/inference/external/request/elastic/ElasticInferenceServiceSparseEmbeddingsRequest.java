/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

//TODO: test
public class ElasticInferenceServiceSparseEmbeddingsRequest implements ElasticInferenceServiceRequest {

    private final List<String> input;

    private final URI uri;

    private final ElasticInferenceServiceModel model;

    public ElasticInferenceServiceSparseEmbeddingsRequest(List<String> input, ElasticInferenceServiceSparseEmbeddingsModel model) {
        this.input = input;
        this.model = Objects.requireNonNull(model);
        this.uri = model.uri();
    }

    @Override
    public HttpRequest createHttpRequest() {
        var httpPost = new HttpPost(uri);
        var requestEntity = Strings.toString(new ElasticInferenceServiceSparseEmbeddingsRequestEntity(input));

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestEntity.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public URI getURI() {
        return this.uri;
    }

    @Override
    public Request truncate() {
        // TODO: truncation
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // TODO: truncation
        return null;
    }

}
