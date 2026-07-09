/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.storage.blob.models.BlobStorageException;

import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps an Azure blob-read stream so a fault <em>while reading the body</em> surfaces as a typed
 * {@link ExternalUnavailableException} the provider-agnostic resume loop can act on.
 * <p>
 * The Azure {@code BlobInputStream} does not let a {@link BlobStorageException} escape the stream: its
 * {@code dispatchRead} catches the {@link BlobStorageException} and rethrows it wrapped in a plain
 * {@link IOException} (its cause). That wrapped {@code IOException} carries no transient/throttle signal, so the
 * resume loop's classifier treats it as a hard error and the resume never engages. We catch that
 * {@code IOException} here and re-type it: the body is opaque object bytes, so a mid-read fault is a transport
 * fault and is always transient. When the cause is a {@link BlobStorageException} we read its status off it so a
 * 429/503 is flagged throttling, matching the open-path classification.
 */
final class AzureTransientTypingInputStream extends FilterInputStream {

    private final StoragePath path;

    AzureTransientTypingInputStream(InputStream delegate, StoragePath path) {
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
        // The throttle status lives on a BlobStorageException in the cause chain (dispatchRead wraps it in a plain
        // IOException; a future SDK may nest it deeper). Walk the chain — matching RetryPolicy.isThrottlingError —
        // rather than reading only the immediate cause. Absent a BlobStorageException, a mid-read transport fault
        // is still transient, just not throttling.
        boolean throttling = false;
        for (Throwable c = e.getCause(); c != null; c = c.getCause()) {
            if (c instanceof BlobStorageException bse) {
                throttling = ExternalUnavailableException.isThrottlingStatus(bse.getStatusCode());
                break;
            }
        }
        return new ExternalUnavailableException(throttling, e, "transient read failure for [{}]", path);
    }
}
