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

import java.util.List;

final class OtelAttributesGetter implements HttpServerAttributesGetter<RequestAndRoute, RestResponse> {

    @Override
    public String getUrlScheme(RequestAndRoute request) {
        return request.request().getHttpRequest().getScheme();
    }

    @Override
    public String getUrlPath(RequestAndRoute request) {
        return request.urlPath();
    }

    @Override
    public String getUrlQuery(RequestAndRoute request) {
        return request.urlQuery();
    }

    @Override
    public String getHttpRoute(RequestAndRoute request) {
        return request.matchedRoute();
    }

    @Override
    public String getHttpRequestMethod(RequestAndRoute request) {
        try {
            return request.request().method().name();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public List<String> getHttpRequestHeader(RequestAndRoute request, String name) {
        return request.request().getHttpRequest().getHeaders().getOrDefault(name, List.of());
    }

    @Override
    public Integer getHttpResponseStatusCode(RequestAndRoute request, RestResponse response, Throwable error) {
        return response.status().getStatus();
    }

    @Override
    public List<String> getHttpResponseHeader(RequestAndRoute request, RestResponse response, String name) {
        return response.getHeaders().getOrDefault(name, List.of());
    }

    @Override
    public String getNetworkProtocolName(RequestAndRoute request, RestResponse response) {
        return "http";
    }

    @Override
    public String getNetworkProtocolVersion(RequestAndRoute request, RestResponse response) {
        return switch (request.request().getHttpRequest().protocolVersion()) {
            case HTTP_1_0 -> "1.0";
            case HTTP_1_1 -> "1.1";
        };
    }
}
