/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tracing.Traceable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
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

    BytesStreamOutput bytesOutput();

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
        return this.request().path();
    }

    @Override
    default Map<String, Object> getAttributes() {
        var req = this.request();
        return Map.of(
            "http.method",
            req.method().name(),
            "http.url",
            req.uri()
        );
    }
}
