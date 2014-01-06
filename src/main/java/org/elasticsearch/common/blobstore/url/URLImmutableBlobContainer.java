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

import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Read-only URL-based implementation of {@link ImmutableBlobContainer}
 */
public class URLImmutableBlobContainer extends AbstractURLBlobContainer implements ImmutableBlobContainer {

    /**
     * Constructs a new URLImmutableBlobContainer
     *
     * @param blobStore blob store
     * @param blobPath  blob path to this container
     * @param path      URL of this container
     */
    public URLImmutableBlobContainer(URLBlobStore blobStore, BlobPath blobPath, URL path) {
        super(blobStore, blobPath, path);
    }

    /**
     * This operation is not supported by URL Blob Container
     */
    @Override
    public void writeBlob(final String blobName, final InputStream is, final long sizeInBytes, final WriterListener listener) {
        throw new UnsupportedOperationException("URL repository is read only");
    }

    /**
     * This operation is not supported by URL Blob Container
     */
    @Override
    public void writeBlob(String blobName, InputStream is, long sizeInBytes) throws IOException {
        throw new UnsupportedOperationException("URL repository is read only");
    }
}
