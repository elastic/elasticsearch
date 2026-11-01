/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.net.URIBuilder;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.common.InferencePreferences;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.elasticsearch.xpack.inference.telemetry.TraceContextHandler;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class ElasticInferenceServiceAuthorizationRequest extends ElasticInferenceServiceRequest {

    private final URI uri;
    private final TraceContextHandler traceContextHandler;
    static final String AUTHORIZATION_PATH = "/api/v2/authorizations";

    public ElasticInferenceServiceAuthorizationRequest(
        String url,
        TraceContext traceContext,
        ElasticInferenceServiceRequestMetadata requestMetadata,
        CCMAuthenticationApplierFactory.AuthApplier authApplier,
        @Nullable InferencePreferences preferences
    ) {
        super(requestMetadata, preferences, authApplier);
        this.uri = createUri(Objects.requireNonNull(url));
        this.traceContextHandler = new TraceContextHandler(traceContext);
    }

    private static URI createUri(String url) throws ElasticsearchStatusException {
        try {
            return new URIBuilder(url).setPath(AUTHORIZATION_PATH).build();
        } catch (URISyntaxException e) {
            throw new ElasticsearchStatusException(
                "Failed to create URI for service [" + ElasticInferenceService.NAME + "]: " + e.getMessage(),
                RestStatus.BAD_REQUEST,
                e
            );
        }
    }

    @Override
    public SimpleHttpRequest createSimpleHttpRequest() {
        var httpGet = SimpleRequestBuilder.get(uri).build();
        traceContextHandler.propagateTraceContext(httpGet);

        return httpGet;
    }

    public TraceContext getTraceContext() {
        return traceContextHandler.traceContext();
    }

    @Override
    public String getInferenceEntityId() {
        // TODO look into refactoring so we don't even need to return this, look at the RetryingHttpSender to fix this
        return "authorization_request";
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
        return null;
    }
}
