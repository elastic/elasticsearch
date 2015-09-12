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

package org.elasticsearch.common.blobstore.support;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public abstract class AbstractBlobContainer implements BlobContainer {

    private final BlobPath path;

    protected AbstractBlobContainer(BlobPath path) {
        this.path = path;
    }

    @Override
    public BlobPath path() {
        return this.path;
    }

    @Override
    public void deleteBlobsByPrefix(final String blobNamePrefix) throws IOException {
        Map<String, BlobMetaData> blobs = listBlobsByPrefix(blobNamePrefix);
        for (BlobMetaData blob : blobs.values()) {
            deleteBlob(blob.name());
        }
    }

    @Override
    public void deleteBlobs(Collection<String> blobNames) throws IOException {
        for(String blob: blobNames) {
            deleteBlob(blob);
        }
    }
}
