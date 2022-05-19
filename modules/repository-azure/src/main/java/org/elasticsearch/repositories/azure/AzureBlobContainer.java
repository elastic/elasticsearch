/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import com.azure.storage.blob.models.BlobStorageException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.Map;

public class AzureBlobContainer extends AbstractBlobContainer {

    private final Logger logger = LogManager.getLogger(AzureBlobContainer.class);
    private final AzureBlobStore blobStore;
    private final String keyPath;

    AzureBlobContainer(BlobPath path, AzureBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.keyPath = path.buildAsString();
    }

    @Override
    public boolean blobExists(String blobName) throws IOException {
        logger.trace("blobExists({})", blobName);
        return blobStore.blobExists(buildKey(blobName));
    }

    private InputStream openInputStream(String blobName, long position, @Nullable Long length) throws IOException {
        String blobKey = buildKey(blobName);
        logger.trace("readBlob({}) from position [{}] with length [{}]", blobName, position, length != null ? length : "unlimited");
        if (blobStore.getLocationMode() == LocationMode.SECONDARY_ONLY && blobExists(blobName) == false) {
            // On Azure, if the location path is a secondary location, and the blob does not
            // exist, instead of returning immediately from the getInputStream call below
            // with a 404 StorageException, Azure keeps trying and trying for a long timeout
            // before throwing a storage exception. This can cause long delays in retrieving
            // snapshots, so we first check if the blob exists before trying to open an input
            // stream to it.
            throw new NoSuchFileException("Blob [" + blobKey + "] not found");
        }
        try {
            return blobStore.getInputStream(blobKey, position, length);
        } catch (Exception e) {
            Throwable rootCause = Throwables.getRootCause(e);
            if (rootCause instanceof BlobStorageException blobStorageException) {
                if (blobStorageException.getStatusCode() == 404) {
                    throw new NoSuchFileException("Blob [" + blobKey + "] not found");
                }
            }
            throw new IOException("Unable to get input stream for blob [" + blobKey + "]", e);
        }
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return openInputStream(blobName, 0L, null);
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        return openInputStream(blobName, position, length);
    }

    @Override
    public long readBlobPreferredLength() {
        return blobStore.getReadChunkSize();
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        logger.trace("writeBlob({}, stream, {})", buildKey(blobName), blobSize);
        blobStore.writeBlob(buildKey(blobName), inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, bytes, failIfAlreadyExists);
    }

    @Override
    public void writeBlob(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        blobStore.writeBlob(buildKey(blobName), bytes, failIfAlreadyExists);
    }

    @Override
    public void writeBlob(String blobName, boolean failIfAlreadyExists, boolean atomic, CheckedConsumer<OutputStream, IOException> writer)
        throws IOException {
        blobStore.writeBlob(buildKey(blobName), failIfAlreadyExists, writer);
    }

    @Override
    public DeleteResult delete() throws IOException {
        return blobStore.deleteBlobDirectory(keyPath);
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames) throws IOException {
        blobStore.deleteBlobs(new Iterator<>() {
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

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(@Nullable String prefix) throws IOException {
        logger.trace("listBlobsByPrefix({})", prefix);
        return blobStore.listBlobsByPrefix(keyPath, prefix);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        logger.trace("listBlobs()");
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        final BlobPath path = path();
        return blobStore.children(path);
    }

    protected String buildKey(String blobName) {
        return keyPath + (blobName == null ? "" : blobName);
    }
}
