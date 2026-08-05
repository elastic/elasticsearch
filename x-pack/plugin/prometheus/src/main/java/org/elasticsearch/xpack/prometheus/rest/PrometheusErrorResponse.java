/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

/**
 * Utility for building and sending Prometheus-format error responses.
 *
 * <p>Error types follow the Prometheus HTTP API specification:
 * <a href="https://github.com/prometheus/prometheus/blob/main/web/api/v1/api.go">api.go</a>
 */
class PrometheusErrorResponse {

    private PrometheusErrorResponse() {}

    /**
     * Sends a Prometheus-format error response derived from the given exception.
     * If sending fails, logs a warning and attempts a plain-text fallback response.
     */
    static void send(RestChannel channel, Exception e, Logger logger) {
        try {
            RestStatus status = ExceptionsHelper.status(e);
            channel.sendResponse(new RestResponse(status, build(status, e.getMessage())));
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.warn("Failed to send error response", inner);
            try {
                channel.sendResponse(
                    new RestResponse(
                        RestStatus.INTERNAL_SERVER_ERROR,
                        RestResponse.TEXT_CONTENT_TYPE,
                        new BytesArray("Internal server error")
                    )
                );
            } catch (Exception ignored) {}
        }
    }

    /**
     * Builds a Prometheus-format error JSON response body:
     * {@code {"status":"error","errorType":"<type>","error":"<message>"}}
     */
    static XContentBuilder build(RestStatus status, String message) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("status", "error");
        builder.field("errorType", mapErrorType(status));
        builder.field("error", message != null ? message : "unknown error");
        builder.endObject();
        return builder;
    }

    /**
     * Maps an HTTP status to a Prometheus error type string.
     */
    static String mapErrorType(RestStatus status) {
        return switch (status) {
            case BAD_REQUEST -> "bad_data";
            case SERVICE_UNAVAILABLE, REQUEST_TIMEOUT, GATEWAY_TIMEOUT -> "timeout";
            default -> "execution";
        };
    }
}
