/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import com.google.cloud.storage.StorageException;

import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps a GCS object-read stream so a fault <em>while reading the body</em> surfaces as a typed
 * {@link ExternalUnavailableException} the provider-agnostic resume loop can act on.
 * <p>
 * The GCS {@code ReadChannel} does not let a {@link StorageException} escape the stream: its {@code read}
 * ({@code BaseStorageReadChannel.read}) catches the {@link StorageException} and rethrows it wrapped in a plain
 * {@link IOException} (its cause). That wrapped {@code IOException} carries no transient/throttle signal, so the
 * resume loop's classifier treats it as a hard error and the whole-object/range resume never engages. We catch
 * that {@code IOException} here and re-type it: the body is opaque object bytes, so a mid-read fault is a
 * transport fault and is always transient (the retry layer re-opens and resumes, exactly as for S3). When the
 * cause is a {@link StorageException} we read its status off it so a 429/503 is flagged throttling, matching the
 * open-path classification.
 */
final class GcsTransientTypingInputStream extends FilterInputStream {

    private final StoragePath path;

    GcsTransientTypingInputStream(InputStream delegate, StoragePath path) {
        super(delegate);
        this.path = path;
    }

    @Override
    public int read() throws IOException {
        try {
            return in.read();
        } catch (IOException e) {
            throw type(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            return in.read(b, off, len);
        } catch (IOException e) {
            throw type(e);
        }
    }

    private ExternalUnavailableException type(IOException e) {
        // The throttle status lives on a StorageException in the cause chain (the ReadChannel wraps it in a plain
        // IOException; a future SDK may nest it deeper). Walk the chain — matching RetryPolicy.isThrottlingError —
        // rather than reading only the immediate cause. Absent a StorageException, a mid-read transport fault is
        // still transient, just not throttling.
        boolean throttling = false;
        for (Throwable c = e.getCause(); c != null; c = c.getCause()) {
            if (c instanceof StorageException se) {
                throttling = ExternalUnavailableException.isThrottlingStatus(se.getCode());
                break;
            }
        }
        return new ExternalUnavailableException(throttling, e, "transient read failure for [{}]", path);
    }
}
