/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.elasticsearch.xpack.inference.telemetry.TraceContextHandler;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class ElasticInferenceServiceAuthorizationRequest extends ElasticInferenceServiceRequest {

    private final URI uri;
    private final TraceContextHandler traceContextHandler;

    public ElasticInferenceServiceAuthorizationRequest(
        String url,
        TraceContext traceContext,
        ElasticInferenceServiceRequestMetadata requestMetadata
    ) {
        super(requestMetadata);
        this.uri = createUri(Objects.requireNonNull(url));
        this.traceContextHandler = new TraceContextHandler(traceContext);
    }

    private URI createUri(String url) throws ElasticsearchStatusException {
        try {
            // TODO, consider transforming the base URL into a URI for better error handling.
            return new URI(url + "/api/v1/authorizations");
        } catch (URISyntaxException e) {
            throw new ElasticsearchStatusException(
                "Failed to create URI for service [" + ElasticInferenceService.NAME + "]: " + e.getMessage(),
                RestStatus.BAD_REQUEST,
                e
            );
        }
    }

    @Override
    public HttpRequestBase createHttpRequestBase() {
        var httpGet = new HttpGet(uri);
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
    public Request truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }

}
