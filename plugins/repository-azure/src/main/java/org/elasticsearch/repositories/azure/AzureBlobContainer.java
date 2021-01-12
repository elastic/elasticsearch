/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.azure;

import com.azure.storage.blob.models.BlobStorageException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        logger.trace("readBlob({}) from position [{}] with length [{}]", blobName, position, length != null ? length : "unlimited");
        if (blobStore.getLocationMode() == LocationMode.SECONDARY_ONLY && !blobExists(blobName)) {
            // On Azure, if the location path is a secondary location, and the blob does not
            // exist, instead of returning immediately from the getInputStream call below
            // with a 404 StorageException, Azure keeps trying and trying for a long timeout
            // before throwing a storage exception.  This can cause long delays in retrieving
            // snapshots, so we first check if the blob exists before trying to open an input
            // stream to it.
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }
        try {
            return blobStore.getInputStream(buildKey(blobName), position, length);
        } catch (Exception e) {
            Throwable rootCause = Throwables.getRootCause(e);
            if (rootCause instanceof BlobStorageException) {
                if (((BlobStorageException) rootCause).getStatusCode() == 404) {
                    throw new NoSuchFileException("Blob [" + blobName + "] not found");
                }
            }
            throw new IOException("Unable to get input stream for blob [" + blobName + "]", e);
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
    public DeleteResult delete() throws IOException {
        return blobStore.deleteBlobDirectory(keyPath);
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        List<String> blobsWithFullPath = blobNames.stream()
            .map(this::buildKey)
            .collect(Collectors.toList());

        blobStore.deleteBlobList(blobsWithFullPath);
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
