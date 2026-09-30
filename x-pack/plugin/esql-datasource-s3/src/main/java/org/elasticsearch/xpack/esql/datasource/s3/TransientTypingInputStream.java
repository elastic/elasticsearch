/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.xpack.esql.datasources.spi.ExternalCredentialsExpiredException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps an S3 object-read stream so that a failure <em>while reading raw bytes</em> is typed for the
 * resume loop instead of a bare {@link IOException} or AWS {@link SdkException}.
 * <p>
 * A transport fault (reset, premature end of body, read timeout, abort) becomes
 * {@link ExternalUnavailableException}. Session-token codes ({@code ExpiredToken},
 * {@code InvalidToken}, {@code TokenRefreshRequired}) become
 * {@link ExternalCredentialsExpiredException}: retrying the same credentials cannot succeed.
 * Classification is by exception type and AWS error code, not message text.
 * <p>
 * Remains {@link Abortable} so a caller's abort fast-path still reaches the underlying S3 stream rather than
 * falling back to a draining {@code close()}.
 */
final class TransientTypingInputStream extends FilterInputStream implements Abortable {

    private final StoragePath path;

    TransientTypingInputStream(InputStream delegate, StoragePath path) {
        super(delegate);
        this.path = path;
    }

    @Override
    public int read() throws IOException {
        try {
            return in.read();
        } catch (IOException | SdkException e) {
            throw wrap(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            return in.read(b, off, len);
        } catch (IOException | SdkException e) {
            throw wrap(e);
        }
    }

    private ExternalException wrap(Exception e) {
        ExternalCredentialsExpiredException expired = S3FailureDetail.expired(e, "reading [" + path + "]");
        if (expired != null) {
            return expired;
        }
        // Remaining mid-body faults are transport (the GET already succeeded). Flag throttling for a
        // rare 503/429 on the body so it shares the throttle budget.
        long retryAfterMs = 0L;
        boolean throttling = false;
        for (Throwable current = e; current != null; current = current.getCause()) {
            if (current instanceof S3Exception s3e) {
                throttling = ExternalUnavailableException.isThrottlingStatus(s3e.statusCode());
                if (throttling && s3e.awsErrorDetails() != null && s3e.awsErrorDetails().sdkHttpResponse() != null) {
                    retryAfterMs = ExternalUnavailableException.parseRetryAfterMs(
                        s3e.awsErrorDetails().sdkHttpResponse().firstMatchingHeader("Retry-After").orElse(null)
                    );
                }
                break;
            }
        }
        return new ExternalUnavailableException(throttling, retryAfterMs, e, "transient read failure for [{}]", path);
    }

    @Override
    public void abort() {
        if (in instanceof Abortable abortable) {
            abortable.abort();
        }
    }
}
