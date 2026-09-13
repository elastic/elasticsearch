/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.pin;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/**
 * Pin probe for plain-HTTPS sources (CloudFront and friends): {@code HEAD} only. {@code list()}
 * throws {@link UnsupportedOperationException} — HTTP cannot enumerate a directory, which is why
 * glob/multi-file layouts are structurally {@code blocked} on this provider.
 */
public class HttpsPinProbe implements PinProbe {

    private final HttpClient client;

    public HttpsPinProbe() {
        this.client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).connectTimeout(Duration.ofSeconds(20)).build();
    }

    @Override
    public ObjectMetadata head(String uri) throws IOException {
        return PinRetry.withRetries("HEAD " + uri, PinRetry.DEFAULT_MAX_ATTEMPTS, () -> doHead(uri));
    }

    private ObjectMetadata doHead(String uri) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(uri))
            .method("HEAD", HttpRequest.BodyPublishers.noBody())
            .timeout(Duration.ofSeconds(30))
            .build();
        HttpResponse<Void> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during HEAD " + uri, e);
        }
        if (response.statusCode() != 200) {
            throw new PinRetry.HttpStatusException("HEAD " + uri + " returned " + response.statusCode(), response.statusCode());
        }
        String etag = response.headers().firstValue("ETag").map(HttpsPinProbe::stripQuotes).orElse(null);
        long size = response.headers().firstValueAsLong("Content-Length").orElse(-1);
        String lastModified = response.headers().firstValue("Last-Modified").orElse(null);
        return new ObjectMetadata(uri, etag, size, lastModified);
    }

    @Override
    public List<ObjectMetadata> list(String uri, int maxKeys) {
        throw new UnsupportedOperationException("HTTP cannot list directories; multi-file layouts are blocked on HTTPS");
    }

    static String stripQuotes(String etag) {
        return etag == null ? null : etag.replace("\"", "");
    }
}
