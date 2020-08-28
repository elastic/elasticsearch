/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative.tool;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple http response with status and response body as key value map. To be
 * used with {@link CommandLineHttpClient}.
 */
final class HttpResponse {
    private final int httpStatus;
    private final Map<String, Object> responseBody;

    HttpResponse(final int httpStatus, final Map<String, Object> responseBody) {
        this.httpStatus = httpStatus;
        Map<String, Object> response = new HashMap<>();
        response.putAll(responseBody);
        this.responseBody = Collections.unmodifiableMap(response);
    }

    int getHttpStatus() {
        return httpStatus;
    }

    Map<String, Object> getResponseBody() {
        return responseBody;
    }

    static class HttpResponseBuilder {
        private int httpStatus;
        private Map<String, Object> responseBody;

        HttpResponseBuilder withHttpStatus(final int httpStatus) {
            this.httpStatus = httpStatus;
            return this;
        }

        HttpResponseBuilder withResponseBody(final String responseJson)
                throws ElasticsearchParseException, UnsupportedEncodingException {
            if (responseJson == null || responseJson.trim().isEmpty()) {
                throw new ElasticsearchParseException(
                        "Invalid string provided as http response body, Failed to parse content to form response body.");
            }
            this.responseBody = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseJson, false);
            return this;
        }

        HttpResponse build() {
            HttpResponse httpResponse = new HttpResponse(this.httpStatus, this.responseBody);
            return httpResponse;
        }
    }
}
