/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.elasticsearch.xpack.inference.telemetry.TraceContextHandler;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * Request adapter for COMPLETION task type that converts simple text inputs
 * into chat message format and sends them to the EIS chat endpoint.
 */
public class ElasticInferenceServiceCompletionRequest extends ElasticInferenceServiceRequest {

    private final ElasticInferenceServiceCompletionModel model;
    private final List<String> inputs;
    private final TraceContextHandler traceContextHandler;

    public ElasticInferenceServiceCompletionRequest(
        List<String> inputs,
        ElasticInferenceServiceCompletionModel model,
        TraceContext traceContext,
        ElasticInferenceServiceRequestMetadata requestMetadata
    ) {
        super(requestMetadata);
        this.inputs = Objects.requireNonNull(inputs);
        this.model = Objects.requireNonNull(model);
        this.traceContextHandler = new TraceContextHandler(traceContext);
    }

    @Override
    public HttpRequestBase createHttpRequestBase() {
        var httpPost = new HttpPost(model.uri());
        var requestEntity = Strings.toString(
            new ElasticInferenceServiceCompletionRequestEntity(inputs, model.getServiceSettings().modelId())
        );

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestEntity.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        traceContextHandler.propagateTraceContext(httpPost);
        httpPost.setHeader(new BasicHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType()));

        return httpPost;
    }

    @Override
    public URI getURI() {
        return model.uri();
    }

    @Override
    public Request truncate() {
        // No truncation
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation
        return null;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public boolean isStreaming() {
        // COMPLETION adapter always uses non-streaming
        return false;
    }
}


