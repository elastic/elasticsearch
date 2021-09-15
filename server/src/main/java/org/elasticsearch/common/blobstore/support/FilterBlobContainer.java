/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.support;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class FilterBlobContainer implements BlobContainer {

    private final BlobContainer delegate;

    public FilterBlobContainer(BlobContainer delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    protected abstract BlobContainer wrapChild(BlobContainer child);

    @Override
    public BlobPath path() {
        return delegate.path();
    }

    @Override
    public boolean blobExists(String blobName) throws IOException {
        return delegate.blobExists(blobName);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return delegate.readBlob(blobName);
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        return delegate.readBlob(blobName, position, length);
    }

    @Override
    public long readBlobPreferredLength() {
        return delegate.readBlobPreferredLength();
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        delegate.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlob(String blobName, boolean failIfAlreadyExists, boolean atomic,
                          CheckedConsumer<OutputStream, IOException> writer) throws IOException {
        delegate.writeBlob(blobName, failIfAlreadyExists, atomic, writer);
    }

    @Override
    public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        delegate.writeBlobAtomic(blobName, bytes, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        return delegate.delete();
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames) throws IOException {
        delegate.deleteBlobsIgnoringIfNotExists(blobNames);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return delegate.listBlobs();
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        return delegate.children().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> wrapChild(e.getValue())));
    }


    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        return delegate.listBlobsByPrefix(blobNamePrefix);
    }
}
