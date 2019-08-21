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

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class GoogleCloudStorageBlobContainer extends AbstractBlobContainer {

    private final GoogleCloudStorageBlobStore blobStore;
    private final String path;

    GoogleCloudStorageBlobContainer(BlobPath path, GoogleCloudStorageBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.path = path.buildAsString();
    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return blobStore.listBlobs(path);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        return blobStore.listChildren(path());
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(String prefix) throws IOException {
        return blobStore.listBlobsByPrefix(path, prefix);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return blobStore.readBlob(buildKey(blobName));
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        blobStore.writeBlob(buildKey(blobName), inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        blobStore.deleteBlob(buildKey(blobName));
    }

    @Override
    public DeleteResult delete() throws IOException {
        return blobStore.deleteDirectory(path().buildAsString());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        blobStore.deleteBlobsIgnoringIfNotExists(blobNames.stream().map(this::buildKey).collect(Collectors.toList()));
    }

    private String buildKey(String blobName) {
        assert blobName != null;
        return path + blobName;
    }
}
