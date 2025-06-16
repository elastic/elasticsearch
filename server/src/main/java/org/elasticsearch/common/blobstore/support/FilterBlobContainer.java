/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A blob container that by default delegates all methods to an internal BlobContainer. Implementations must define {@link #wrapChild} so
 * that the abstraction is complete: so that the internal BlobContainer instance cannot leak out of this wrapper.
 *
 * Inheritors can safely modify needed methods while continuing to have access to a complete BlobContainer implementation beneath.
 */
public abstract class FilterBlobContainer implements BlobContainer {

    private final BlobContainer delegate;

    public FilterBlobContainer(BlobContainer delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    /**
     * Wraps up any instances of the internal BlobContainer type in another BlobContainer type (presumably the implementation's type).
     * Ensures that the internal {@link #delegate} type never leaks out of the BlobContainer wrapper type.
     */
    protected abstract BlobContainer wrapChild(BlobContainer child);

    @Override
    public BlobPath path() {
        return delegate.path();
    }

    @Override
    public boolean blobExists(OperationPurpose purpose, String blobName) throws IOException {
        return delegate.blobExists(purpose, blobName);
    }

    @Override
    public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
        return delegate.readBlob(purpose, blobName);
    }

    @Override
    public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
        return delegate.readBlob(purpose, blobName, position, length);
    }

    @Override
    public long readBlobPreferredLength() {
        return delegate.readBlobPreferredLength();
    }

    @Override
    public void writeBlob(OperationPurpose purpose, String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws IOException {
        delegate.writeBlob(purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeMetadataBlob(
        OperationPurpose purpose,
        String blobName,
        boolean failIfAlreadyExists,
        boolean atomic,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException {
        delegate.writeMetadataBlob(purpose, blobName, failIfAlreadyExists, atomic, writer);
    }

    @Override
    public boolean supportsConcurrentMultipartUploads() {
        return delegate.supportsConcurrentMultipartUploads();
    }

    @Override
    public void writeBlobAtomic(
        OperationPurpose purpose,
        String blobName,
        long blobSize,
        BlobMultiPartInputStreamProvider provider,
        boolean failIfAlreadyExists
    ) throws IOException {
        delegate.writeBlobAtomic(purpose, blobName, blobSize, provider, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(
        OperationPurpose purpose,
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists
    ) throws IOException {
        delegate.writeBlobAtomic(purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists)
        throws IOException {
        delegate.writeBlobAtomic(purpose, blobName, bytes, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete(OperationPurpose purpose) throws IOException {
        return delegate.delete(purpose);
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) throws IOException {
        delegate.deleteBlobsIgnoringIfNotExists(purpose, blobNames);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
        return delegate.listBlobs(purpose);
    }

    @Override
    public Map<String, BlobContainer> children(OperationPurpose purpose) throws IOException {
        return delegate.children(purpose).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> wrapChild(e.getValue())));
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) throws IOException {
        return delegate.listBlobsByPrefix(purpose, blobNamePrefix);
    }

    @Override
    public void compareAndExchangeRegister(
        OperationPurpose purpose,
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<OptionalBytesReference> listener
    ) {
        delegate.compareAndExchangeRegister(purpose, key, expected, updated, listener);
    }

    @Override
    public void compareAndSetRegister(
        OperationPurpose purpose,
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<Boolean> listener
    ) {
        delegate.compareAndSetRegister(purpose, key, expected, updated, listener);
    }

    @Override
    public void getRegister(OperationPurpose purpose, String key, ActionListener<OptionalBytesReference> listener) {
        delegate.getRegister(purpose, key, listener);
    }
}
