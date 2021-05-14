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

import java.io.IOException;

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
    public void close() throws IOException {
        delegate.close();
    }

    protected BlobStore delegate() {
        return delegate;
    }

}
