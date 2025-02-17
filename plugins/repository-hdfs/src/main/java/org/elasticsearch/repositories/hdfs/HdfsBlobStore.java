/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.hdfs;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;

import java.io.IOException;

final class HdfsBlobStore implements BlobStore {

    private final Path root;
    private final FileContext fileContext;
    private final HdfsSecurityContext securityContext;
    private final int bufferSize;
    private final boolean readOnly;
    private final Short replicationFactor;
    private volatile boolean closed;

    HdfsBlobStore(FileContext fileContext, String path, int bufferSize, boolean readOnly) throws IOException {
        this(fileContext, path, bufferSize, readOnly, false, null);
    }

    HdfsBlobStore(FileContext fileContext, String path, int bufferSize, boolean readOnly, boolean haEnabled, Short replicationFactor)
        throws IOException {
        this.fileContext = fileContext;
        // Only restrict permissions if not running with HA
        boolean restrictPermissions = (haEnabled == false);
        this.securityContext = new HdfsSecurityContext(fileContext.getUgi(), restrictPermissions);
        this.bufferSize = bufferSize;
        this.replicationFactor = replicationFactor;
        this.root = execute(fileContext1 -> fileContext1.makeQualified(new Path(path)));
        this.readOnly = readOnly;
        if (readOnly == false) {
            try {
                mkdirs(root);
            } catch (FileAlreadyExistsException ok) {
                // behaves like Files.createDirectories
            }
        }
    }

    @SuppressWarnings("HiddenField")
    private void mkdirs(Path path) throws IOException {
        execute((Operation<Void>) fileContext -> {
            fileContext.mkdir(path, null, true);
            return null;
        });
    }

    @Override
    public String toString() {
        return root.toUri().toString();
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new HdfsBlobContainer(path, this, buildHdfsPath(path), bufferSize, securityContext, replicationFactor);
    }

    private Path buildHdfsPath(BlobPath blobPath) {
        final Path path = translateToHdfsPath(blobPath);
        if (readOnly == false) {
            try {
                mkdirs(path);
            } catch (FileAlreadyExistsException ok) {
                // behaves like Files.createDirectories
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to create blob container", ex);
            }
        }
        return path;
    }

    private Path translateToHdfsPath(BlobPath blobPath) {
        Path path = root;
        for (String p : blobPath.parts()) {
            path = new Path(path, p);
        }
        return path;
    }

    interface Operation<V> {
        V run(FileContext fileContext) throws IOException;
    }

    /**
     * Executes the provided operation against this store
     */
    <V> V execute(Operation<V> operation) throws IOException {
        if (closed) {
            throw new AlreadyClosedException("HdfsBlobStore is closed: " + this);
        }
        return securityContext.doPrivilegedOrThrow(() -> {
            securityContext.ensureLogin();
            return operation.run(fileContext);
        });
    }

    @Override
    public void close() {
        closed = true;
    }
}
