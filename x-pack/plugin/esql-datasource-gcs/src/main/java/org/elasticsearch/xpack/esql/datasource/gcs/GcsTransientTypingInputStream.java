/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import com.google.cloud.storage.StorageException;

import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.TransientStorageException;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps a GCS object-read stream so a fault <em>while reading the body</em> surfaces as a typed
 * {@link TransientStorageException} rather than the unchecked {@link StorageException} the GCS client throws.
 * <p>
 * Without this, a mid-read network fault escapes as an unchecked exception that the provider-agnostic resume
 * loop's {@code catch (IOException)} never sees, so the whole-object/range resume cannot engage. Re-typing it
 * here (the body is opaque object bytes, so a read fault is a transport fault) lets the retry layer re-open and
 * resume exactly as it does for S3. A 429/503 surfaced during the read is flagged throttling, matching the
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
        } catch (StorageException e) {
            throw type(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            return in.read(b, off, len);
        } catch (StorageException e) {
            throw type(e);
        }
    }

    private TransientStorageException type(StorageException e) {
        boolean throttling = e.getCode() == 503 || e.getCode() == 429;
        return new TransientStorageException("transient read failure for " + path, e, throttling);
    }
}
