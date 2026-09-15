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
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.xpack.inference.common.InferencePreferences;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRerankRequest;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.elasticsearch.xpack.inference.telemetry.TraceContextHandler;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class ElasticInferenceServiceRerankRequest extends ElasticInferenceServiceRequest implements OutboundRerankRequest {

    private final InferenceString query;
    private final List<InferenceString> documents;
    private final Integer topN;
    private final TraceContextHandler traceContextHandler;
    private final ElasticInferenceServiceRerankModel model;

    public ElasticInferenceServiceRerankRequest(
        InferenceString query,
        List<InferenceString> documents,
        Integer topN,
        ElasticInferenceServiceRerankModel model,
        TraceContext traceContext,
        ElasticInferenceServiceRequestMetadata metadata,
        InferencePreferences preferences,
        CCMAuthenticationApplierFactory.AuthApplier authApplier
    ) {
        super(metadata, preferences, authApplier);
        this.query = query;
        this.documents = documents;
        this.topN = topN;
        this.model = Objects.requireNonNull(model);
        this.traceContextHandler = new TraceContextHandler(traceContext);
    }

    @Override
    public SimpleHttpRequest createSimpleHttpRequest() {
        var httpPost = SimpleRequestBuilder.post(getURI()).build();
        var requestEntity = Strings.toString(
            new ElasticInferenceServiceRerankRequestEntity(query, documents, model.getServiceSettings().modelId(), topN)
        );

        httpPost.setBody(requestEntity.getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);

        traceContextHandler.propagateTraceContext(httpPost);

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
    public OutboundRequest truncate() {
        // no truncation
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // no truncation
        return null;
    }
}
