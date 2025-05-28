/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic.rerank;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.request.elastic.ElasticInferenceServiceRequest;
import org.elasticsearch.xpack.inference.external.request.elastic.ElasticInferenceServiceRequestMetadata;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.elasticsearch.xpack.inference.telemetry.TraceContextHandler;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class ElasticInferenceServiceRerankRequest extends ElasticInferenceServiceRequest {

    private final String query;
    private final List<String> documents;
    private final TraceContextHandler traceContextHandler;
    private final ElasticInferenceServiceRerankModel model;

    public ElasticInferenceServiceRerankRequest(
        String query,
        List<String> documents,
        ElasticInferenceServiceRerankModel model,
        TraceContext traceContext,
        ElasticInferenceServiceRequestMetadata metadata
    ) {
        super(metadata);
        this.query = query;
        this.documents = documents;
        this.model = Objects.requireNonNull(model);
        this.traceContextHandler = new TraceContextHandler(traceContext);
    }

    @Override
    public HttpRequestBase createHttpRequestBase() {
        var httpPost = new HttpPost(getURI());
        var requestEntity = Strings.toString(
            new ElasticInferenceServiceRerankRequestEntity(
                query,
                documents,
                model.getServiceSettings().modelId(),
                model.getTaskSettings().getTopNDocumentsOnly()
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
        return model.uri();
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
}
