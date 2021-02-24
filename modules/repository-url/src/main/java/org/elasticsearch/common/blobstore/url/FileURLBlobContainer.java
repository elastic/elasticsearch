/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url;

import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class FileURLBlobContainer extends URLBlobContainer {
    public FileURLBlobContainer(URLBlobStore blobStore, BlobPath blobPath, URL path) {
        super(blobStore, blobPath, path);
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        final InputStream inputStream = getInputStream(new URL(path, blobName));
        // This can be extremely inefficient for jar and ftp URLs
        if (position > 0) {
            inputStream.skip(position);
        }
        return Streams.limitStream(inputStream, length);
    }
}
