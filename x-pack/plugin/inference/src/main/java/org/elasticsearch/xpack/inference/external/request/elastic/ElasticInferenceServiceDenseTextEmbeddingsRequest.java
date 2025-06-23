/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.elasticsearch.xpack.inference.telemetry.TraceContextHandler;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.elastic.ElasticInferenceServiceSparseEmbeddingsRequest.inputTypeToUsageContext;

public class ElasticInferenceServiceDenseTextEmbeddingsRequest extends ElasticInferenceServiceRequest {

    private final URI uri;
    private final ElasticInferenceServiceDenseTextEmbeddingsModel model;
    private final List<String> inputs;
    private final TraceContextHandler traceContextHandler;
    private final InputType inputType;

    public ElasticInferenceServiceDenseTextEmbeddingsRequest(
        ElasticInferenceServiceDenseTextEmbeddingsModel model,
        List<String> inputs,
        TraceContext traceContext,
        ElasticInferenceServiceRequestMetadata metadata,
        InputType inputType
    ) {
        super(metadata);
        this.inputs = inputs;
        this.model = Objects.requireNonNull(model);
        this.uri = model.uri();
        this.traceContextHandler = new TraceContextHandler(traceContext);
        this.inputType = inputType;
    }

    @Override
    public HttpRequestBase createHttpRequestBase() {
        var httpPost = new HttpPost(uri);
        var usageContext = inputTypeToUsageContext(inputType);
        var requestEntity = Strings.toString(
            new ElasticInferenceServiceDenseTextEmbeddingsRequestEntity(inputs, model.getServiceSettings().modelId(), usageContext)
        );

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestEntity.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        traceContextHandler.propagateTraceContext(httpPost);
        httpPost.setHeader(new BasicHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType()));

        return httpPost;
    }

    public TraceContext getTraceContext() {
        return traceContextHandler.traceContext();
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
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }
}
