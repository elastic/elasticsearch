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
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;

/**
 *
 */
public class BlobStoreWrapper implements BlobStore {

    private BlobStore delegate;

    public BlobStoreWrapper(BlobStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return delegate.blobContainer(path);
    }

    @Override
    public void delete(BlobPath path) {
        delegate.delete(path);
    }

    @Override
    public void close() {
        delegate.close();
    }

    protected BlobStore delegate() {
        return delegate;
    }

}
