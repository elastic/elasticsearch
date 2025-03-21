/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Counter.OPERATION;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Counter.OPERATION_EXCEPTION;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Operation.INSERT_OBJECT;

class GoogleCloudStorageBlobContainer extends AbstractBlobContainer {

    private final GoogleCloudStorageBlobStore blobStore;
    private final GoogleCloudStorageOperationsStats tracker;
    private final String path;

    GoogleCloudStorageBlobContainer(BlobPath path, GoogleCloudStorageBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.path = path.buildAsString();
        this.tracker = blobStore.tracker();
    }

    @Override
    public boolean blobExists(OperationPurpose purpose, String blobName) {
        try {
            return blobStore.blobExists(purpose, buildKey(blobName));
        } catch (Exception e) {
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
        return blobStore.listBlobs(purpose, path);
    }

    @Override
    public Map<String, BlobContainer> children(OperationPurpose purpose) throws IOException {
        return blobStore.listChildren(purpose, path());
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String prefix) throws IOException {
        return blobStore.listBlobsByPrefix(purpose, path, prefix);
    }

    @Override
    public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
        return blobStore.readBlob(purpose, buildKey(blobName));
    }

    @Override
    public InputStream readBlob(OperationPurpose purpose, final String blobName, final long position, final long length)
        throws IOException {
        return blobStore.readBlob(purpose, buildKey(blobName), position, length);
    }

    // We don't track InsertObject operation on the http layer as
    // we do with the GET/LIST operations since this operations
    // can trigger multiple underlying http requests but only one
    // operation is billed.
    private void trackInsertObject(OperationPurpose purpose, CheckedRunnable<IOException> r) throws IOException {
        try {
            r.run();
            tracker.inc(purpose, INSERT_OBJECT, OPERATION);
        } catch (Exception e) {
            tracker.inc(purpose, INSERT_OBJECT, OPERATION_EXCEPTION);
            throw e;
        }
    }

    @Override
    public void writeBlob(OperationPurpose purpose, String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws IOException {
        trackInsertObject(
            purpose,
            () -> blobStore.writeStreamBlob(purpose, buildKey(blobName), inputStream, blobSize, failIfAlreadyExists)
        );
    }

    @Override
    public void writeBlob(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        trackInsertObject(purpose, () -> blobStore.writeFullBlob(purpose, buildKey(blobName), bytes, failIfAlreadyExists));
    }

    @Override
    public void writeMetadataBlob(
        OperationPurpose purpose,
        String blobName,
        boolean failIfAlreadyExists,
        boolean atomic,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException {
        trackInsertObject(purpose, () -> blobStore.writeMetadata(purpose, buildKey(blobName), failIfAlreadyExists, writer));
    }

    @Override
    public void writeBlobAtomic(
        OperationPurpose purpose,
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists
    ) throws IOException {
        writeBlob(purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists)
        throws IOException {
        writeBlob(purpose, blobName, bytes, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete(OperationPurpose purpose) throws IOException {
        return blobStore.deleteDirectory(purpose, path().buildAsString());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) throws IOException {
        blobStore.deleteBlobs(purpose, new Iterator<>() {
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
        OperationPurpose purpose,
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<OptionalBytesReference> listener
    ) {
        ActionListener.completeWith(
            listener,
            () -> blobStore.compareAndExchangeRegister(purpose, buildKey(key), path, key, expected, updated)
        );
    }

    @Override
    public void getRegister(OperationPurpose purpose, String key, ActionListener<OptionalBytesReference> listener) {
        ActionListener.completeWith(listener, () -> blobStore.getRegister(purpose, buildKey(key), path, key));
    }
}
