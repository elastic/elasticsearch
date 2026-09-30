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
import org.apache.http.message.BasicHeader;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.InferencePreferences;
import org.elasticsearch.xpack.inference.external.request.OutboundDocumentExtractionRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.elasticsearch.xpack.inference.telemetry.TraceContextHandler;

import java.net.URI;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.jsonEntity;

/**
 * A request to the Elastic Inference Service document extraction endpoint.
 */
public class ElasticInferenceServiceDocumentExtractionRequest extends ElasticInferenceServiceRequest
    implements
        OutboundDocumentExtractionRequest {

    private final List<InferenceString> documents;
    private final TraceContextHandler traceContextHandler;
    private final ElasticInferenceServiceDocumentExtractionModel model;

    public ElasticInferenceServiceDocumentExtractionRequest(
        List<InferenceString> documents,
        ElasticInferenceServiceDocumentExtractionModel model,
        TraceContext traceContext,
        ElasticInferenceServiceRequestMetadata metadata,
        InferencePreferences preferences,
        CCMAuthenticationApplierFactory.AuthApplier authApplier
    ) {
        super(metadata, preferences, authApplier);
        this.documents = Objects.requireNonNull(documents);
        this.model = Objects.requireNonNull(model);
        this.traceContextHandler = new TraceContextHandler(traceContext);
    }

    @Override
    public HttpRequestBase createHttpRequestBase() {
        var httpPost = new HttpPost(getURI());
        httpPost.setEntity(
            jsonEntity(
                new ElasticInferenceServiceDocumentExtractionRequestEntity(
                    documents,
                    model.getServiceSettings().modelId(),
                    model.getTaskSettings().outputFormat()
                )
            )
        );

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
