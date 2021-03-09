/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url;

import org.elasticsearch.common.blobstore.BlobPath;

import java.io.InputStream;
import java.net.URL;

public class FileURLBlobContainer extends URLBlobContainer {
    public FileURLBlobContainer(URLBlobStore blobStore, BlobPath blobPath, URL path) {
        super(blobStore, blobPath, path);
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) {
        throw new UnsupportedOperationException("URL repository doesn't support this operation. Please use a 'fs' repository instead");
    }
}
