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

package org.elasticsearch.common.blobstore.url;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * URL blob implementation of {@link org.elasticsearch.common.blobstore.BlobContainer}
 */
public abstract class AbstractURLBlobContainer extends AbstractBlobContainer {

    protected final URLBlobStore blobStore;

    protected final URL path;

    /**
     * Constructs new AbstractURLBlobContainer
     *
     * @param blobStore blob store
     * @param blobPath  blob path for this container
     * @param path      URL for this container
     */
    public AbstractURLBlobContainer(URLBlobStore blobStore, BlobPath blobPath, URL path) {
        super(blobPath);
        this.blobStore = blobStore;
        this.path = path;
    }

    /**
     * Returns URL for this container
     *
     * @return URL for this container
     */
    public URL url() {
        return this.path;
    }

    /**
     * This operation is not supported by AbstractURLBlobContainer
     */
    @Override
    public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
        throw new UnsupportedOperationException("URL repository doesn't support this operation");
    }

    /**
     * This operation is not supported by AbstractURLBlobContainer
     */
    @Override
    public boolean deleteBlob(String blobName) throws IOException {
        throw new UnsupportedOperationException("URL repository is read only");
    }

    /**
     * This operation is not supported by AbstractURLBlobContainer
     */
    @Override
    public boolean blobExists(String blobName) {
        throw new UnsupportedOperationException("URL repository doesn't support this operation");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readBlob(final String blobName, final ReadBlobListener listener) {
        blobStore.executor().execute(new Runnable() {
            @Override
            public void run() {
                byte[] buffer = new byte[blobStore.bufferSizeInBytes()];
                InputStream is = null;
                try {
                    is = new URL(path, blobName).openStream();
                    int bytesRead;
                    while ((bytesRead = is.read(buffer)) != -1) {
                        listener.onPartial(buffer, 0, bytesRead);
                    }
                } catch (Throwable t) {
                    IOUtils.closeWhileHandlingException(is);
                    listener.onFailure(t);
                    return;
                }
                try {
                    IOUtils.closeWhileHandlingException(is);
                    listener.onCompleted();
                } catch (Throwable t) {
                    listener.onFailure(t);
                }
            }
        });
    }
}
