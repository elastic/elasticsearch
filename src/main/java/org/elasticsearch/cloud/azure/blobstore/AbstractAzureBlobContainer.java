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

package org.elasticsearch.cloud.azure.blobstore;

import com.microsoft.windowsazure.services.core.ServiceException;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;

/**
 *
 */
public class AbstractAzureBlobContainer extends AbstractBlobContainer {

    protected final ESLogger logger = ESLoggerFactory.getLogger(AbstractAzureBlobContainer.class.getName());
    protected final AzureBlobStore blobStore;

    protected final String keyPath;

    public AbstractAzureBlobContainer(BlobPath path, AzureBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        String keyPath = path.buildAsString("/");
        if (!keyPath.isEmpty()) {
            keyPath = keyPath + "/";
        }
        this.keyPath = keyPath;
    }

    @Override
    public boolean blobExists(String blobName) {
        try {
            return blobStore.client().blobExists(blobStore.container(), buildKey(blobName));
        } catch (URISyntaxException e) {
            logger.warn("can not access [{}] in container {{}}: {}", blobName, blobStore.container(), e.getMessage());
        } catch (StorageException e) {
            logger.warn("can not access [{}] in container {{}}: {}", blobName, blobStore.container(), e.getMessage());
        }
        return false;
    }

    @Override
    public boolean deleteBlob(String blobName) throws IOException {
        try {
            blobStore.client().deleteBlob(blobStore.container(), buildKey(blobName));
            return true;
        } catch (URISyntaxException e) {
            logger.warn("can not access [{}] in container {{}}: {}", blobName, blobStore.container(), e.getMessage());
            throw new IOException(e);
        } catch (StorageException e) {
            logger.warn("can not access [{}] in container {{}}: {}", blobName, blobStore.container(), e.getMessage());
            throw new IOException(e);
        }
    }

    @Override
    public void readBlob(final String blobName, final ReadBlobListener listener) {
        blobStore.executor().execute(new Runnable() {
            @Override
            public void run() {
            InputStream is = null;
            try {
                is = blobStore.client().getInputStream(blobStore.container(), buildKey(blobName));
                byte[] buffer = new byte[blobStore.bufferSizeInBytes()];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    listener.onPartial(buffer, 0, bytesRead);
                }
                is.close();
                listener.onCompleted();
            } catch (ServiceException e) {
                if (e.getHttpStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                    listener.onFailure(new FileNotFoundException(e.getMessage()));
                } else {
                    listener.onFailure(e);
                }
            } catch (Throwable e) {
                IOUtils.closeWhileHandlingException(is);
                listener.onFailure(e);
            }
            }
        });
    }

    @Override
    public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(@Nullable String prefix) throws IOException {

        try {
            return blobStore.client().listBlobsByPrefix(blobStore.container(), keyPath, prefix);
        } catch (URISyntaxException e) {
            logger.warn("can not access [{}] in container {{}}: {}", prefix, blobStore.container(), e.getMessage());
            throw new IOException(e);
        } catch (StorageException e) {
            logger.warn("can not access [{}] in container {{}}: {}", prefix, blobStore.container(), e.getMessage());
            throw new IOException(e);
        } catch (ServiceException e) {
            logger.warn("can not access [{}] in container {{}}: {}", prefix, blobStore.container(), e.getMessage());
            throw new IOException(e);
        }
    }

    @Override
    public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    protected String buildKey(String blobName) {
        return keyPath + blobName;
    }
}
