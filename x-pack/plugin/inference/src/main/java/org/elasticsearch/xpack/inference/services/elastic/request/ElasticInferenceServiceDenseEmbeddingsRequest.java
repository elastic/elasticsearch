/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.message.BasicHeader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.common.InferencePreferences;
import org.elasticsearch.xpack.inference.external.request.OutboundDenseEmbeddingRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceUsageContext;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.elasticsearch.xpack.inference.telemetry.TraceContextHandler;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER;

public class ElasticInferenceServiceDenseEmbeddingsRequest extends ElasticInferenceServiceRequest implements OutboundDenseEmbeddingRequest {

    private final URI uri;
    private final ElasticInferenceServiceDenseEmbeddingsModel model;
    private final List<InferenceStringGroup> inputs;
    private final TraceContextHandler traceContextHandler;
    private final InputType inputType;

    public ElasticInferenceServiceDenseEmbeddingsRequest(
        ElasticInferenceServiceDenseEmbeddingsModel model,
        List<InferenceStringGroup> inputs,
        TraceContext traceContext,
        ElasticInferenceServiceRequestMetadata metadata,
        InputType inputType,
        InferencePreferences preferences,
        CCMAuthenticationApplierFactory.AuthApplier authApplier
    ) {
        super(metadata, preferences, authApplier);
        this.inputs = inputs;
        this.model = Objects.requireNonNull(model);
        this.uri = model.uri();
        this.traceContextHandler = new TraceContextHandler(traceContext);
        this.inputType = inputType;
    }

    @Override
    public SimpleHttpRequest createSimpleHttpRequest() {
        var httpPost = SimpleRequestBuilder.post(uri).build();
        var usageContext = ElasticInferenceServiceUsageContext.fromInputType(inputType);
        var requestEntity = Strings.toString(new ElasticInferenceServiceDenseEmbeddingsRequestEntity(inputs, model, usageContext));

        httpPost.setBody(requestEntity.getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);

        traceContextHandler.propagateTraceContext(httpPost);
        httpPost.setHeader(new BasicHeader(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, usageContext.productUseCaseHeaderValue()));

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
    public OutboundRequest truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }

    @Override
    public TaskType getTaskType() {
        return model.getTaskType();
    }
}
