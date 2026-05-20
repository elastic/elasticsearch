/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.instrumentation;

import io.opentelemetry.instrumentation.api.semconv.http.HttpServerAttributesGetter;

import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.telemetry.instrumentation.HttpServerSemConvAttributes;

import java.util.List;

/**
 * Maps ES {@link RequestAndRoute} and {@link RestResponse} to the OTel HTTP server semconv attributes
 * expected by {@link io.opentelemetry.instrumentation.api.semconv.http.HttpServerAttributesExtractor}.
 * <p>
 * Proxy-header resolution (RFC 7239 {@code Forwarded}, {@code X-Forwarded-*}) is delegated to
 * {@link HttpServerSemConvAttributes} rather than duplicated here.
 */
final class OtelAttributesGetter implements HttpServerAttributesGetter<RequestAndRoute, RestResponse> {

    @Override
    public String getUrlScheme(RequestAndRoute requestAndRoute) {
        final String forwarded = HttpServerSemConvAttributes.firstHeaderValue(requestAndRoute.request(), "Forwarded");
        return HttpServerSemConvAttributes.extractScheme(requestAndRoute.request(), forwarded);
    }

    @Override
    public String getUrlPath(RequestAndRoute requestAndRoute) {
        final String uri = requestAndRoute.uri();
        final int queryIdx = uri.indexOf('?');
        return queryIdx >= 0 ? uri.substring(0, queryIdx) : uri;
    }

    @Override
    public String getUrlQuery(RequestAndRoute requestAndRoute) {
        final String uri = requestAndRoute.uri();
        final int queryIdx = uri.indexOf('?');
        return queryIdx >= 0 ? uri.substring(queryIdx + 1) : null;
    }

    @Override
    public String getHttpRoute(RequestAndRoute requestAndRoute) {
        return requestAndRoute.matchedRoute();
    }

    @Override
    public String getHttpRequestMethod(RequestAndRoute requestAndRoute) {
        try {
            return requestAndRoute.request().method().name();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public List<String> getHttpRequestHeader(RequestAndRoute requestAndRoute, String name) {
        return requestAndRoute.request().getHeaders().getOrDefault(name, List.of());
    }

    @Override
    public Integer getHttpResponseStatusCode(RequestAndRoute requestAndRoute, RestResponse response, Throwable error) {
        return response.status().getStatus();
    }

    @Override
    public List<String> getHttpResponseHeader(RequestAndRoute requestAndRoute, RestResponse response, String name) {
        return response.getHeaders().getOrDefault(name, List.of());
    }

    @Override
    public String getNetworkProtocolName(RequestAndRoute requestAndRoute, RestResponse response) {
        return "http";
    }

    @Override
    public String getNetworkProtocolVersion(RequestAndRoute requestAndRoute, RestResponse response) {
        return switch (requestAndRoute.request().getHttpRequest().protocolVersion()) {
            case HTTP_1_0 -> "1.0";
            case HTTP_1_1 -> "1.1";
        };
    }

}
