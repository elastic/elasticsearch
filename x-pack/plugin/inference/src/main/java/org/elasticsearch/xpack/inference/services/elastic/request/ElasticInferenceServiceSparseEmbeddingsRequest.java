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
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceUsageContext;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.elasticsearch.xpack.inference.telemetry.TraceContextHandler;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class ElasticInferenceServiceSparseEmbeddingsRequest extends ElasticInferenceServiceRequest {

    private final URI uri;
    private final ElasticInferenceServiceSparseEmbeddingsModel model;
    private final Truncator.TruncationResult truncationResult;
    private final Truncator truncator;
    private final TraceContextHandler traceContextHandler;
    private final InputType inputType;

    public ElasticInferenceServiceSparseEmbeddingsRequest(
        Truncator truncator,
        Truncator.TruncationResult truncationResult,
        ElasticInferenceServiceSparseEmbeddingsModel model,
        TraceContext traceContext,
        ElasticInferenceServiceRequestMetadata metadata,
        InputType inputType
    ) {
        super(metadata);
        this.truncator = truncator;
        this.truncationResult = truncationResult;
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
            new ElasticInferenceServiceSparseEmbeddingsRequestEntity(
                truncationResult.input(),
                model.getServiceSettings().modelId(),
                usageContext
            )
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
        var truncatedInput = truncator.truncate(truncationResult.input());
        return new ElasticInferenceServiceSparseEmbeddingsRequest(
            truncator,
            truncatedInput,
            model,
            traceContextHandler.traceContext(),
            getMetadata(),
            inputType
        );
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }

    // visible for testing
    static ElasticInferenceServiceUsageContext inputTypeToUsageContext(InputType inputType) {
        switch (inputType) {
            case SEARCH -> {
                return ElasticInferenceServiceUsageContext.SEARCH;
            }
            case INGEST -> {
                return ElasticInferenceServiceUsageContext.INGEST;
            }
            default -> {
                return ElasticInferenceServiceUsageContext.UNSPECIFIED;
            }
        }
    }
}
