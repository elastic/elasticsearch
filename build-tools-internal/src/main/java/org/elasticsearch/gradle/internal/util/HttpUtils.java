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
import java.net.URI;

public final class HttpUtils {

    private static final int HTTP_READ_MAX_ATTEMPTS = 3;
    private static final long HTTP_READ_RETRY_BACKOFF_MILLIS = 1000L;

    private HttpUtils() {}

    @FunctionalInterface
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    public static byte[] readHttpBytesWithRetry(String url) throws IOException {
        return readHttpBytesWithRetry(url, HTTP_READ_MAX_ATTEMPTS, HTTP_READ_RETRY_BACKOFF_MILLIS, Thread::sleep);
    }

    public static byte[] readHttpBytesWithRetry(String url, int maxAttempts, long baseBackoffMillis, Sleeper sleeper) throws IOException {
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
                    throw new IOException("Interrupted while retrying download from: " + url, e);
                }
            }

            try (InputStream in = URI.create(url).toURL().openStream()) {
                return in.readAllBytes();
            } catch (IOException e) {
                lastException = e;
            }
        }
        assert lastException != null;
        throw lastException;
    }
}
