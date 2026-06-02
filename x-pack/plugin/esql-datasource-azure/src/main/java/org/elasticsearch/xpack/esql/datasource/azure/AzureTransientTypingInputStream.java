/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.storage.blob.models.BlobStorageException;

import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.TransientStorageException;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps an Azure blob-read stream so a fault <em>while reading the body</em> surfaces as a typed
 * {@link TransientStorageException} rather than the unchecked {@link BlobStorageException} the Azure client
 * throws — otherwise the mid-read fault escapes the provider-agnostic resume loop's {@code catch (IOException)}
 * and the resume cannot engage. The body is opaque object bytes, so a read fault is a transport fault; a
 * 429/503 surfaced during the read is flagged throttling, matching the open-path classification.
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
        } catch (BlobStorageException e) {
            throw type(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            return in.read(b, off, len);
        } catch (BlobStorageException e) {
            throw type(e);
        }
    }

    private TransientStorageException type(BlobStorageException e) {
        boolean throttling = e.getStatusCode() == 503 || e.getStatusCode() == 429;
        return new TransientStorageException("transient read failure for " + path, e, throttling);
    }
}
