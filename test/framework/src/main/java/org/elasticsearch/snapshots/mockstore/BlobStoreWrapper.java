/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots.mockstore;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

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
    public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) throws IOException {
        delegate.deleteBlobsIgnoringIfNotExists(purpose, blobNames);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public Map<String, Long> stats() {
        return delegate.stats();
    }

    public BlobStore delegate() {
        return delegate;
    }

}
