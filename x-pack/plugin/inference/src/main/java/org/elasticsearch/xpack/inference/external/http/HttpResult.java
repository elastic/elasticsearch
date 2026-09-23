/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.http.HttpResponse;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.common.SizeLimitInputStream;

import java.io.IOException;
import java.util.Objects;

public record HttpResult(HttpResponse response, byte[] body) {

    public static HttpResult create(ByteSizeValue maxResponseSize, SimpleHttpResponse response) throws IOException {
        return new HttpResult(response, limitBody(maxResponseSize, response));
    }

    private static byte[] limitBody(ByteSizeValue maxResponseSize, SimpleHttpResponse response) throws IOException {
        var bodyBytes = response.getBodyBytes();
        if (bodyBytes == null) {
            return new byte[0];
        }

        // The response is already fully buffered in memory at this point, so a plain length check suffices; the exception type
        // (and message) match SizeLimitInputStream, which the retry logic relies on when the limit is exceeded.
        if (bodyBytes.length > maxResponseSize.getBytes()) {
            throw new SizeLimitInputStream.InputStreamTooLargeException(
                "Maximum limit of [" + maxResponseSize.getBytes() + "] bytes reached"
            );
        }

        return bodyBytes;
    }

    public HttpResult {
        Objects.requireNonNull(response);
        Objects.requireNonNull(body);
    }

    public boolean isBodyEmpty() {
        return body().length == 0;
    }

    public boolean isSuccessfulResponse() {
        return RestStatus.isSuccessful(response.getCode());
    }
}
