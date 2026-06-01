/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Iterator;
import java.util.List;

public class FsBlobStore implements BlobStore {
    protected final Logger logger = LogManager.getLogger(getClass());

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
                createDirExposingRace(f);
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to create blob container", ex);
            }
        }
        return new FsBlobContainer(this, path, f);
    }

    private static void createDirExposingRace(Path dir) throws IOException {
        try {
            createAndCheckIsDirectory(dir);
        } catch (FileAlreadyExistsException x) {
            // file exists and is not a directory
            throw x;
        } catch (IOException x) {
            // parent may not exist or other reason
        }

        SecurityException se = null;
        Path absDir = dir;
        try {
            absDir = dir.toAbsolutePath();
        } catch (SecurityException x) {
            // don't have permission to get absolute path
            se = x;
        }
        // find a descendant that exists
        Path parent = absDir.getParent();
        while (parent != null) {
            try {
                parent.getFileSystem().provider().checkAccess(parent);
                break;
            } catch (NoSuchFileException x) {
                // does not exist
            }
            parent = parent.getParent();
        }
        if (parent == null) {
            // unable to find existing parent
            if (se == null) {
                throw new FileSystemException(absDir.toString(), null, "Unable to determine if root directory exists");
            } else {
                throw se;
            }
        }

        // create directories
        Path child = parent;
        for (Path name : parent.relativize(absDir)) {
            child = child.resolve(name);
            createAndCheckIsDirectory(child);
        }
    }

    private static void createAndCheckIsDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        try {
            Files.createDirectory(dir, attrs);
        } catch (FileAlreadyExistsException x) {
            try {
                Thread.sleep(Randomness.createSecure().nextInt(1000));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            if (!Files.isDirectory(dir)) throw x;
        }
    }

    void deleteBlobs(Iterator<String> blobNames) throws IOException {
        IOException ioe = null;
        long suppressedExceptions = 0;
        while (blobNames.hasNext()) {
            try {
                // FsBlobContainer uses this method to delete blobs; in that case each blob name is already an absolute path meaning that
                // the resolution done here is effectively a non-op.
                Path resolve = path.resolve(blobNames.next());
                logger.info("[deleteBlobs] thread={} rm {}", Thread.currentThread().getName(), resolve);
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

    public Path buildPath(BlobPath path) {
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
