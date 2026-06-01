/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.TransientStorageException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * Translates an S3/SDK failure into the storage SPI's exception vocabulary so the provider-agnostic retry
 * layer can classify it by <em>type</em>, not by message text. The knowledge of which S3 status codes are
 * transient lives here, in the provider, where the concrete SDK types are on the classpath.
 * <ul>
 *   <li><b>Throttling</b> (HTTP 429 / 503, including {@code SlowDown}) → {@link TransientStorageException}
 *       flagged {@code throttling=true}, so the retry policy grants the larger throttle budget and feeds the
 *       cross-request adaptive backoff.</li>
 *   <li><b>Other 5xx</b> and transport faults (connection reset, read timeout) → {@link TransientStorageException}
 *       (plain transient).</li>
 *   <li><b>4xx</b> (auth, not-found, bad-range, …) → a plain {@link IOException}, which is <em>not</em> retried.</li>
 * </ul>
 */
final class S3Faults {

    private S3Faults() {}

    /**
     * Maps {@code cause} to the right SPI exception. {@code context} is a short human description of the
     * operation that failed (e.g. {@code "Range request failed"}), used in the message.
     */
    static IOException classify(Throwable cause, StoragePath path, String context) {
        if (cause instanceof NoSuchKeyException) {
            return new IOException("Object not found: " + path, cause);
        }
        if (cause instanceof S3Exception s3e) {
            int status = s3e.statusCode();
            if (status == 503 || status == 429) {
                // 503 covers SlowDown; 429 is TooManyRequests. Both are throttling.
                return new TransientStorageException(context + " for " + path, cause, true);
            }
            if (status >= 500) {
                return new TransientStorageException(context + " for " + path, cause, false);
            }
            // 4xx — permission, missing, malformed, unsatisfiable range — is not retryable.
            return new IOException(context + " for " + path, cause);
        }
        // Transport faults surfacing below the HTTP layer.
        if (cause instanceof SocketException || cause instanceof SocketTimeoutException || cause instanceof InterruptedIOException) {
            return new TransientStorageException(context + " for " + path, cause, false);
        }
        if (cause instanceof IOException io) {
            return io;
        }
        return new IOException(context + " for " + path, cause);
    }
}
