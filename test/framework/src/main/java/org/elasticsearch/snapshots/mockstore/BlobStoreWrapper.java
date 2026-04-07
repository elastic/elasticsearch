/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots.mockstore;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;

import java.io.IOException;
import java.util.Map;

public class BlobStoreWrapper implements BlobStore {

    private final BlobStore delegate;

    public BlobStoreWrapper(BlobStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return delegate.blobContainer(path);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public Map<String, BlobStoreActionStats> stats() {
        return delegate.stats();
    }

    public BlobStore delegate() {
        return delegate;
    }

}
