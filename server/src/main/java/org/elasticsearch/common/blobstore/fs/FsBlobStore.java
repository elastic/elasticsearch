/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore.fs;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.exception.ElasticsearchException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
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
        Path f = buildPath(path);
        if (readOnly == false) {
            try {
                Files.createDirectories(f);
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to create blob container", ex);
            }
        }
        return new FsBlobContainer(this, path, f);
    }

    void deleteBlobs(Iterator<String> blobNames) throws IOException {
        IOException ioe = null;
        long suppressedExceptions = 0;
        while (blobNames.hasNext()) {
            try {
                // FsBlobContainer uses this method to delete blobs; in that case each blob name is already an absolute path meaning that
                // the resolution done here is effectively a non-op.
                Path resolve = path.resolve(blobNames.next());
                IOUtils.rm(resolve);
            } catch (IOException e) {
                // IOUtils.rm puts the original exception as a string in the IOException message. Ignore no such file exception.
                if (e.getMessage().contains("NoSuchFileException") == false) {
                    // track up to 10 delete exceptions and try to continue deleting on exceptions
                    if (ioe == null) {
                        ioe = e;
                    } else if (ioe.getSuppressed().length < 10) {
                        ioe.addSuppressed(e);
                    } else {
                        ++suppressedExceptions;
                    }
                }
            }
        }
        if (ioe != null) {
            if (suppressedExceptions > 0) {
                ioe.addSuppressed(new IOException("Failed to delete files, suppressed [" + suppressedExceptions + "] failures"));
            }
            throw ioe;
        }
    }

    @Override
    public void close() {
        // nothing to do here...
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
