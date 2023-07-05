/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

class GoogleCloudStorageBlobContainer extends AbstractBlobContainer {

    private final GoogleCloudStorageBlobStore blobStore;
    private final String path;

    GoogleCloudStorageBlobContainer(BlobPath path, GoogleCloudStorageBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.path = path.buildAsString();
    }

    @Override
    public boolean blobExists(String blobName) {
        try {
            return blobStore.blobExists(buildKey(blobName));
        } catch (Exception e) {
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return blobStore.listBlobs(path);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        return blobStore.listChildren(path());
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String prefix) throws IOException {
        return blobStore.listBlobsByPrefix(path, prefix);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return blobStore.readBlob(buildKey(blobName));
    }

    @Override
    public InputStream readBlob(final String blobName, final long position, final long length) throws IOException {
        return blobStore.readBlob(buildKey(blobName), position, length);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        blobStore.writeBlob(buildKey(blobName), inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlob(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        blobStore.writeBlob(buildKey(blobName), bytes, failIfAlreadyExists);
    }

    @Override
    public void writeMetadataBlob(
        String blobName,
        boolean failIfAlreadyExists,
        boolean atomic,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException {
        blobStore.writeBlob(buildKey(blobName), failIfAlreadyExists, writer);
    }

    @Override
    public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, bytes, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        return blobStore.deleteDirectory(path().buildAsString());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames) throws IOException {
        blobStore.deleteBlobsIgnoringIfNotExists(new Iterator<>() {
            @Override
            public boolean hasNext() {
                return blobNames.hasNext();
            }

            @Override
            public String next() {
                return buildKey(blobNames.next());
            }
        });
    }

    private String buildKey(String blobName) {
        assert blobName != null;
        return path + blobName;
    }

    @Override
    public void compareAndExchangeRegister(
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<OptionalBytesReference> listener
    ) {
        if (skipCas(listener)) return;
        ActionListener.completeWith(listener, () -> blobStore.compareAndExchangeRegister(buildKey(key), path, key, expected, updated));
    }

    @Override
    public void getRegister(String key, ActionListener<OptionalBytesReference> listener) {
        if (skipCas(listener)) return;
        ActionListener.completeWith(listener, () -> blobStore.getRegister(buildKey(key), path, key));
    }
}
