/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tracing.Traceable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * A channel used to construct bytes / builder based outputs, and send responses.
 */
public interface RestChannel extends Traceable {

    XContentBuilder newBuilder() throws IOException;

    XContentBuilder newErrorBuilder() throws IOException;

    XContentBuilder newBuilder(@Nullable XContentType xContentType, boolean useFiltering) throws IOException;

    XContentBuilder newBuilder(@Nullable XContentType xContentType, @Nullable XContentType responseContentType, boolean useFiltering)
        throws IOException;

    BytesStream bytesOutput();

    RestRequest request();

    /**
     * @return true iff an error response should contain additional details like exception traces.
     */
    boolean detailedErrorsEnabled();

    void sendResponse(RestResponse response);

    @Override
    default String getSpanId() {
        return "rest-" + this.request().getRequestId();
    }

    @Override
    default String getSpanName() {
        final String tracePath = this.getTracePath();
        return this.request().method() + " " + (tracePath != null ? tracePath : this.request().path());
    }

    @Override
    default Map<String, Object> getAttributes() {
        final RestRequest req = this.request();
        Map<String, Object> attributes = new HashMap<>();
        req.getHeaders().forEach((key, values) -> {
            final String lowerKey = key.toLowerCase(Locale.ROOT).replace('-', '_');
            final String value = switch (lowerKey) {
                case "authorization", "cookie", "secret", "session", "set_cookie", "token" -> "[REDACTED]";
                default -> String.join("; ", values);
            };
            attributes.put("http.request.headers." + lowerKey, value);
        });
        attributes.put("http.method", req.method().name());
        attributes.put("http.url", req.uri());
        switch (req.getHttpRequest().protocolVersion()) {
            case HTTP_1_0 -> attributes.put("http.flavour", "1.0");
            case HTTP_1_1 -> attributes.put("http.flavour", "1.1");
        }
        return attributes;
    }

    void setTracePath(String path);

    String getTracePath();

    default void startTrace() {}

    default void stopTrace() {}

    default void recordException(Throwable throwable) {}
}
