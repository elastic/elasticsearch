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
package org.elasticsearch.repositories.hdfs;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;

import java.io.IOException;

final class HdfsBlobStore implements BlobStore {

    private final Path root;
    private final FileContext fileContext;
    private final HdfsSecurityContext securityContext;
    private final int bufferSize;
    private volatile boolean closed;

    HdfsBlobStore(FileContext fileContext, String path, int bufferSize) throws IOException {
        this(fileContext, path, bufferSize, false);
    }

    HdfsBlobStore(FileContext fileContext, String path, int bufferSize, boolean haEnabled) throws IOException {
        this.fileContext = fileContext;
        // Only restrict permissions if not running with HA
        boolean restrictPermissions = (haEnabled == false);
        this.securityContext = new HdfsSecurityContext(fileContext.getUgi(), restrictPermissions);
        this.bufferSize = bufferSize;
        this.root = execute(new Operation<Path>() {
          @Override
          public Path run(FileContext fileContext) throws IOException {
              return fileContext.makeQualified(new Path(path));
          }
        });
        try {
            mkdirs(root);
        } catch (FileAlreadyExistsException ok) {
            // behaves like Files.createDirectories
        }
    }

    private void mkdirs(Path path) throws IOException {
        execute(new Operation<Void>() {
            @Override
            public Void run(FileContext fileContext) throws IOException {
                fileContext.mkdir(path, null, true);
                return null;
            }
        });
    }

    @Override
    public void delete(BlobPath path) throws IOException {
        execute(new Operation<Void>() {
            @Override
            public Void run(FileContext fc) throws IOException {
                fc.delete(translateToHdfsPath(path), true);
                return null;
            }
        });
    }

    @Override
    public String toString() {
        return root.toUri().toString();
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new HdfsBlobContainer(path, this, buildHdfsPath(path), bufferSize, this.securityContext);
    }

    private Path buildHdfsPath(BlobPath blobPath) {
        final Path path = translateToHdfsPath(blobPath);
        try {
            mkdirs(path);
        } catch (FileAlreadyExistsException ok) {
            // behaves like Files.createDirectories
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to create blob container", ex);
        }
        return path;
    }

    private Path translateToHdfsPath(BlobPath blobPath) {
        Path path = root;
        for (String p : blobPath) {
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
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }
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
