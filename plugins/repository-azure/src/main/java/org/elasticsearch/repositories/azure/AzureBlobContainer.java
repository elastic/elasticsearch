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

import com.microsoft.azure.storage.Constants;
import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.StorageException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class AzureBlobContainer extends AbstractBlobContainer {

    private final Logger logger = LogManager.getLogger(AzureBlobContainer.class);
    private final AzureBlobStore blobStore;
    private final ThreadPool threadPool;
    private final String keyPath;

    AzureBlobContainer(BlobPath path, AzureBlobStore blobStore, ThreadPool threadPool) {
        super(path);
        this.blobStore = blobStore;
        this.keyPath = path.buildAsString();
        this.threadPool = threadPool;
    }

    @Override
    public boolean blobExists(String blobName) {
        logger.trace("blobExists({})", blobName);
        try {
            return blobStore.blobExists(buildKey(blobName));
        } catch (URISyntaxException | StorageException e) {
            logger.warn("can not access [{}] in container {{}}: {}", blobName, blobStore, e.getMessage());
        }
        return false;
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
        } catch (StorageException e) {
            if (e.getHttpStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                throw new NoSuchFileException(e.getMessage());
            }
            throw new IOException(e);
        } catch (URISyntaxException e) {
            throw new IOException(e);
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
        return Constants.DEFAULT_MINIMUM_READ_SIZE_IN_BYTES;
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        logger.trace("writeBlob({}, stream, {})", buildKey(blobName), blobSize);
        try {
            blobStore.writeBlob(buildKey(blobName), inputStream, blobSize, failIfAlreadyExists);
        } catch (URISyntaxException|StorageException e) {
            throw new IOException("Can not write blob " + blobName, e);
        }
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        try {
            return blobStore.deleteBlobDirectory(keyPath, threadPool.executor(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME));
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        final PlainActionFuture<Void> result = PlainActionFuture.newFuture();
        if (blobNames.isEmpty()) {
            result.onResponse(null);
        } else {
            final GroupedActionListener<Void> listener =
                new GroupedActionListener<>(ActionListener.map(result, v -> null), blobNames.size());
            final ExecutorService executor = threadPool.executor(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME);
            // Executing deletes in parallel since Azure SDK 8 is using blocking IO while Azure does not provide a bulk delete API endpoint
            // TODO: Upgrade to newer non-blocking Azure SDK 11 and execute delete requests in parallel that way.
            for (String blobName : blobNames) {
                executor.execute(ActionRunnable.run(listener, () -> {
                    logger.trace("deleteBlob({})", blobName);
                    try {
                        blobStore.deleteBlob(buildKey(blobName));
                    } catch (StorageException e) {
                        if (e.getHttpStatusCode() != HttpURLConnection.HTTP_NOT_FOUND) {
                            throw new IOException(e);
                        }
                    } catch (URISyntaxException e) {
                        throw new IOException(e);
                    }
                }));
            }
        }
        try {
            result.actionGet();
        } catch (Exception e) {
            throw new IOException("Exception during bulk delete", e);
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(@Nullable String prefix) throws IOException {
        logger.trace("listBlobsByPrefix({})", prefix);

        try {
            return blobStore.listBlobsByPrefix(keyPath, prefix);
        } catch (URISyntaxException | StorageException e) {
            logger.warn("can not access [{}] in container {{}}: {}", prefix, blobStore, e.getMessage());
            throw new IOException(e);
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        logger.trace("listBlobs()");
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        final BlobPath path = path();
        try {
            return blobStore.children(path);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException("Failed to list children in path [" + path.buildAsString() + "].", e);
        }
    }

    protected String buildKey(String blobName) {
        return keyPath + (blobName == null ? "" : blobName);
    }
}
