/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Map;
import java.util.function.IntPredicate;

public final class HttpUtils {

    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final long DEFAULT_BACKOFF_MILLIS = 1000L;

    private static final IntPredicate RETRY_UNLESS_OK = status -> status != HttpURLConnection.HTTP_OK;

    private HttpUtils() {}

    @FunctionalInterface
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    /** A timeout of {@code 0} waits indefinitely. */
    public record Request(String method, String url, byte[] body, Map<String, String> headers, int connectTimeout, int readTimeout) {

        public static Request get(String url) {
            return get(url, 0, 0);
        }

        public static Request get(String url, int connectTimeout, int readTimeout) {
            return new Request("GET", url, null, Map.of(), connectTimeout, readTimeout);
        }

        public static Request put(String url, byte[] body, Map<String, String> headers, int connectTimeout, int readTimeout) {
            return new Request("PUT", url, body, headers, connectTimeout, readTimeout);
        }
    }

    /** The HTTP status, and the response body in case of success, or the error message (or empty) in case of failure. */
    public record Response(int status, byte[] body) {}

    public static byte[] readHttpBytesWithRetry(String url) throws IOException {
        return readHttpBytesWithRetry(url, DEFAULT_MAX_ATTEMPTS, DEFAULT_BACKOFF_MILLIS, Thread::sleep);
    }

    public static byte[] readHttpBytesWithRetry(String url, int maxAttempts, long baseBackoffMillis, Sleeper sleeper) throws IOException {
        Response response = sendWithRetry(Request.get(url), maxAttempts, baseBackoffMillis, sleeper, RETRY_UNLESS_OK);
        if (response.status() != HttpURLConnection.HTTP_OK) {
            throw new IOException("Unexpected status " + response.status() + " reading " + url);
        }
        return response.body();
    }

    public static Response sendWithRetry(Request request, Sleeper sleeper, IntPredicate retryableStatus) throws IOException {
        return sendWithRetry(request, DEFAULT_MAX_ATTEMPTS, DEFAULT_BACKOFF_MILLIS, sleeper, retryableStatus);
    }

    /**
     * Sends the request, using {@code retryableStatus} to decide whether to retry (transient failure) or not.
     * In the former case, the request is retried until it succeeds or the attempts run out.
     *
     * @param retryableStatus given a response status, whether another send should be attempted
     * @throws IOException in case of a permanent failure
     * @return the last attempt's response
     */
    public static Response sendWithRetry(
        Request request,
        int maxAttempts,
        long baseBackoffMillis,
        Sleeper sleeper,
        IntPredicate retryableStatus
    ) throws IOException {
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be >= 1 but was [" + maxAttempts + "]");
        }
        if (baseBackoffMillis < 0) {
            throw new IllegalArgumentException("baseBackoffMillis must be >= 0 but was [" + baseBackoffMillis + "]");
        }

        IOException lastException = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (attempt > 1 && baseBackoffMillis > 0) {
                long backoff = baseBackoffMillis * (attempt - 1);
                try {
                    sleeper.sleep(backoff);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while retrying " + request.method() + " " + request.url(), e);
                }
            }

            try {
                Response response = send(request);
                if (retryableStatus.test(response.status()) == false || attempt == maxAttempts) {
                    return response;
                }
            } catch (IOException e) {
                lastException = e;
            }
        }
        assert lastException != null;
        throw lastException;
    }

    /** The response body in case of success, or the error message (or empty) in case of failure. */
    private static byte[] readBody(HttpURLConnection connection, int status) throws IOException {
        if (status / 100 == 2) {
            try (InputStream in = connection.getInputStream()) {
                return in.readAllBytes();
            }
        }
        InputStream errorStream = connection.getErrorStream();
        if (errorStream == null) {
            return new byte[0];
        }
        try (InputStream in = errorStream) {
            return in.readAllBytes();
        } catch (IOException e) {
            return new byte[0];
        }
    }

    private static Response send(Request request) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) URI.create(request.url()).toURL().openConnection();
        try {
            connection.setRequestMethod(request.method());
            connection.setConnectTimeout(request.connectTimeout());
            connection.setReadTimeout(request.readTimeout());
            request.headers().forEach(connection::setRequestProperty);

            if (request.body() != null) {
                connection.setDoOutput(true);
                try (OutputStream out = connection.getOutputStream()) {
                    out.write(request.body());
                }
            }

            int status = connection.getResponseCode();
            return new Response(status, readBody(connection, status));
        } finally {
            connection.disconnect();
        }
    }
}
