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

import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps an S3 object-read stream so that any failure <em>while reading raw bytes</em> is surfaced as a
 * typed {@link ExternalUnavailableException} rather than a bare {@link IOException} or AWS {@link SdkException}.
 * <p>
 * At this layer there is no parsing — the bytes are an opaque object range — so a read failure here is
 * almost always a transport fault (connection reset, premature end of body, read timeout, aborted stream).
 * Classifying it by <b>type</b> here, where the concrete SDK/transport types are on the classpath, lets the
 * generic retry machinery decide to re-open and resume without sniffing exception messages. A rare
 * data-integrity failure (e.g. an SDK checksum mismatch) surfaced on read would also be typed transient and
 * retried, but it simply re-trips on the immutable object and fails within the bounded retry budget.
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

    private ExternalUnavailableException wrap(Exception e) {
        // A mid-body read fault is a transport fault (the request already succeeded), so it is always transient;
        // flag throttling for the rare 503/429 surfaced during the body read so it shares the throttle budget.
        boolean throttling = e instanceof S3Exception s3e && ExternalUnavailableException.isThrottlingStatus(s3e.statusCode());
        return new ExternalUnavailableException(throttling, e, "transient read failure for [{}]", path);
    }

    @Override
    public void abort() {
        if (in instanceof Abortable abortable) {
            abortable.abort();
        }
    }
}
