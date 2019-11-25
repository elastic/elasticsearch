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
package org.elasticsearch.snapshots.mockstore;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class BlobContainerWrapper implements BlobContainer {
    private BlobContainer delegate;

    public BlobContainerWrapper(BlobContainer delegate) {
        this.delegate = delegate;
    }

    @Override
    public BlobPath path() {
        return delegate.path();
    }

    @Override
    public InputStream readBlob(String name) throws IOException {
        return delegate.readBlob(name);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        delegate.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(final String blobName, final InputStream inputStream, final long blobSize,
                                boolean failIfAlreadyExists) throws IOException {
        delegate.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        delegate.deleteBlob(blobName);
    }

    @Override
    public DeleteResult delete() throws IOException {
        return delegate.delete();
    }

    @Override
    public void deleteBlobIgnoringIfNotExists(final String blobName) throws IOException {
        delegate.deleteBlobIgnoringIfNotExists(blobName);
    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return delegate.listBlobs();
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        return delegate.children();
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        return delegate.listBlobsByPrefix(blobNamePrefix);
    }
}
