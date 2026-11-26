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

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalCredentialsExpiredException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;

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
 * falling back to a draining {@code close()}. {@link #close()} itself aborts when the unread remainder is
 * larger than {@link #MAX_TRAILING_DRAIN_BYTES}, the body length is unknown, or a body read already
 * failed. Apache {@code close()} of a just-failed socket can stall another timeout before resume.
 * A small leftover on a healthy stream is drained and {@code onDrained} credits the bytes actually
 * read. Abort and thrown drain do not credit expected leftover: resume would fetch those bytes again.
 */
final class TransientTypingInputStream extends FilterInputStream implements Abortable {

    /**
     * Leftover GET bytes at or below this {@code close()} the inner stream instead of aborting.
     * Apache {@code close()} drains that leftover onto the socket; {@code onDrained} credits
     * bytes actually read after a successful drain of a healthy stream. Larger leftover,
     * unknown length, or a failed body read aborts the connection instead.
     * Keep in sync with {@code DecompressingStorageObject.MAX_TRAILING_DRAIN_BYTES} (64 KiB) and
     * Hadoop S3A readahead. Different package; do not import that package-private field.
     */
    static final int MAX_TRAILING_DRAIN_BYTES = 64 * 1024;

    private final StoragePath path;
    /** {@code < 0} means unknown length: {@link #close()} always aborts. */
    private final long expectedLength;
    /**
     * Bytes delivered to the caller. A single reader thread updates this; a close from another
     * thread may see a stale (lower) count, overestimate leftover, and bias toward abort.
     * Harmless for LIMIT remainders, which are huge enough to abort anyway.
     */
    private volatile long bytesRead;
    private final AtomicBoolean terminal = new AtomicBoolean();
    /** Set when a body read failed; {@link #close()} aborts instead of draining the failed socket. */
    private volatile boolean readFailed;
    @Nullable
    private final LongConsumer onDrained;

    TransientTypingInputStream(InputStream delegate, StoragePath path) {
        this(delegate, path, -1L, null);
    }

    TransientTypingInputStream(InputStream delegate, StoragePath path, long expectedLength) {
        this(delegate, path, expectedLength, null);
    }

    TransientTypingInputStream(InputStream delegate, StoragePath path, long expectedLength, @Nullable LongConsumer onDrained) {
        super(delegate);
        this.path = path;
        this.expectedLength = expectedLength;
        this.onDrained = onDrained;
    }

    @Override
    public int read() throws IOException {
        try {
            int b = in.read();
            if (b >= 0) {
                bytesRead++;
            }
            return b;
        } catch (IOException | SdkException e) {
            throw wrap(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            int n = in.read(b, off, len);
            if (n > 0) {
                bytesRead += n;
            }
            return n;
        } catch (IOException | SdkException e) {
            throw wrap(e);
        }
    }

    /**
     * {@link FilterInputStream#skip} calls {@code in.skip} and bypasses {@link #read()}, so skip
     * bytes must be counted here. bzip2 uses {@code skipNBytes}, which calls {@code skip}.
     */
    @Override
    public long skip(long n) throws IOException {
        try {
            long skipped = in.skip(n);
            if (skipped > 0) {
                bytesRead += skipped;
            }
            return skipped;
        } catch (IOException | SdkException e) {
            throw wrap(e);
        }
    }

    private ExternalException wrap(Exception e) {
        readFailed = true;
        ExternalCredentialsExpiredException expired = S3FailureDetail.expired(e, "reading [" + path.objectName() + "]");
        if (expired != null) {
            return expired;
        }
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
        return new ExternalUnavailableException(
            throttling ? ExternalException.Condition.STORE_THROTTLED : ExternalException.Condition.STORE_UNAVAILABLE,
            path,
            "",
            "",
            throttling,
            retryAfterMs,
            e
        );
    }

    @Override
    public void close() throws IOException {
        long leftover = expectedLength < 0 ? Long.MAX_VALUE : expectedLength - bytesRead;
        // Unknown length always aborts, including leftover 0: without Content-Length we cannot
        // treat EOF as a complete window. S3 almost always sends length; callers that need pool
        // reuse on a fully-read window must pass expectedLength. A failed read also aborts:
        // Apache close() of that socket can stall resume/cancel another timeout.
        if (expectedLength < 0 || leftover > MAX_TRAILING_DRAIN_BYTES || readFailed) {
            abort();
            return;
        }
        if (terminal.getAndSet(true)) {
            return;
        }
        long before = bytesRead;
        try {
            byte[] buf = new byte[(int) Math.min(Math.max(leftover, 1L), 8192L)];
            int n;
            while ((n = in.read(buf)) >= 0) {
                bytesRead += n;
            }
        } catch (IOException | SdkException e) {
            if (in instanceof Abortable abortable) {
                abortable.abort();
            }
            if (e instanceof IOException ioe) {
                throw ioe;
            }
            throw wrap(e);
        }
        publishDrained(bytesRead - before);
        super.close();
    }

    private void publishDrained(long got) {
        if (onDrained != null && got > 0) {
            onDrained.accept(got);
        }
    }

    @Override
    public void abort() {
        if (terminal.getAndSet(true) == false) {
            if (in instanceof Abortable abortable) {
                abortable.abort();
            } else {
                try {
                    // Production S3 inner is AWS Abortable (drop conn). A non-Abortable test
                    // double falls through to close(), which may drain — keep S3 unit tests on
                    // Abortable inners.
                    in.close();
                } catch (IOException ignored) {
                    // abort is best-effort; a noisy close must not fail the caller
                }
            }
        }
    }
}
