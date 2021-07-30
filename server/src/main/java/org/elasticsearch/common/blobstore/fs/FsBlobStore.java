/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.fs;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class FsBlobStore implements BlobStore {

    private final Path path;

    private final int bufferSizeInBytes;

    private final boolean readOnly;

    public FsBlobStore(int bufferSizeInBytes, Path path, boolean readonly) throws IOException {
        this.path = path;
        this.readOnly = readonly;
        if (this.readOnly == false) {
            Files.createDirectories(path);
        }
        this.bufferSizeInBytes = bufferSizeInBytes;
    }

    @Override
    public String toString() {
        return path.toString();
    }

    public Path path() {
        return path;
    }

    public int bufferSizeInBytes() {
        return this.bufferSizeInBytes;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        try {
            return new FsBlobContainer(this, path, buildAndCreate(path));
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to create blob container", ex);
        }
    }

    @Override
    public void close() {
        // nothing to do here...
    }

    private synchronized Path buildAndCreate(BlobPath path) throws IOException {
        Path f = buildPath(path);
        if (readOnly == false) {
            Files.createDirectories(f);
        }
        return f;
    }

    private Path buildPath(BlobPath path) {
        List<String> paths = path.parts();
        if (paths.isEmpty()) {
            return path();
        }
        Path blobPath = this.path.resolve(paths.get(0));
        for (int i = 1; i < paths.size(); i++) {
            blobPath = blobPath.resolve(paths.get(i));
        }
        return blobPath;
    }
}
