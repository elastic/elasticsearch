/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Streams;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.common.SizeLimitInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public record HttpResult(HttpResponse response, byte[] body) {

    public static HttpResult create(ByteSizeValue maxResponseSize, HttpResponse response) throws IOException {
        return new HttpResult(response, limitBody(maxResponseSize, response));
    }

    private static byte[] limitBody(ByteSizeValue maxResponseSize, HttpResponse response) throws IOException {
        if (response.getEntity() == null) {
            return new byte[0];
        }

        final byte[] body;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            try (InputStream is = new SizeLimitInputStream(maxResponseSize, response.getEntity().getContent())) {
                Streams.copy(is, outputStream);
            }
            body = outputStream.toByteArray();
        }

        return body;
    }

    public HttpResult {
        Objects.requireNonNull(response);
        Objects.requireNonNull(body);
    }

    public boolean isBodyEmpty() {
        return body().length == 0;
    }

    public boolean isSuccessfulResponse() {
        return RestStatus.isSuccessful(response.getStatusLine().getStatusCode());
    }
}
