/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link ExtractionBackend} that delegates extraction to an external
 * <a href="https://tika.apache.org/2.x/server.html">tika-server</a> via HTTP.
 *
 * <p>Documents are sent as raw bytes in the body of a {@code PUT /tika/json} request. The server
 * responds with a flat JSON object containing both the extracted text (under the
 * {@code X-TIKA:content} key) and all metadata fields. Multi-valued metadata fields are returned
 * as JSON arrays; this backend takes only the first value.
 *
 * <p>The underlying {@link HttpClient} is shared across all extract calls. Call {@link #close()}
 * when the backend is no longer needed to release HTTP connection resources.
 */
final class TikaServerExtractionBackend implements ExtractionBackend {

    /** JSON key for extracted plain text in tika-server's {@code /tika/json} response. */
    static final String TIKA_CONTENT_KEY = "X-TIKA:content";

    private final HttpClient httpClient;
    private final URI tikaJsonUri;
    private final Duration requestTimeout;

    TikaServerExtractionBackend(URI baseUri, Duration requestTimeout, Duration connectTimeout) {
        this.tikaJsonUri = baseUri.resolve("/tika/json");
        this.requestTimeout = requestTimeout;
        this.httpClient = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
    }

    @Override
    public void extract(byte[] content, @Nullable String resourceName, int maxChars, ActionListener<ExtractionResult> listener) {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(tikaJsonUri)
            .PUT(HttpRequest.BodyPublishers.ofByteArray(content))
            .header("Content-Type", "application/octet-stream")
            .header("Accept", "application/json")
            .header("writeLimit", String.valueOf(maxChars))
            .timeout(requestTimeout);

        if (resourceName != null) {
            requestBuilder.header("Content-Disposition", "attachment; filename=\"" + resourceName + "\"");
        }

        HttpRequest request = requestBuilder.build();

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream()).whenComplete((response, throwable) -> {
            if (throwable != null) {
                listener.onFailure(throwable instanceof Exception e ? e : new RuntimeException(throwable));
                return;
            }
            if (response.statusCode() != 200) {
                listener.onFailure(
                    new IOException("tika-server returned HTTP status [" + response.statusCode() + "] for URI [" + tikaJsonUri + "]")
                );
                return;
            }
            try {
                listener.onResponse(parseResponse(response.body(), maxChars));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private static ExtractionResult parseResponse(InputStream responseBody, int maxChars) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, responseBody)) {
            Map<String, Object> json = parser.map();

            String content = "";
            Object rawContent = json.get(TIKA_CONTENT_KEY);
            if (rawContent instanceof String s) {
                // Client-side truncation as a safety net in addition to the writeLimit header
                content = (maxChars > 0 && s.length() > maxChars) ? s.substring(0, maxChars) : s;
            }

            Map<String, String> metadata = new HashMap<>();
            for (Map.Entry<String, Object> entry : json.entrySet()) {
                if (TIKA_CONTENT_KEY.equals(entry.getKey())) {
                    continue;
                }
                if (entry.getValue() instanceof String s) {
                    metadata.put(entry.getKey(), s);
                } else if (entry.getValue() instanceof List<?> list && list.isEmpty() == false) {
                    // Multi-valued field: take the first value
                    if (list.get(0) instanceof String s) {
                        metadata.put(entry.getKey(), s);
                    }
                }
            }

            return new ExtractionResult(content, metadata);
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
