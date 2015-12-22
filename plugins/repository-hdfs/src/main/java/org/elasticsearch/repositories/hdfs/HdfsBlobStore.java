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
import java.lang.reflect.ReflectPermission;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.AuthPermission;

final class HdfsBlobStore implements BlobStore {

    private final Path root;
    private final FileContext fileContext;
    private final int bufferSize;
    private volatile boolean closed;

    HdfsBlobStore(FileContext fileContext, String path, int bufferSize) throws IOException {
        this.fileContext = fileContext;
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
        return new HdfsBlobContainer(path, this, buildHdfsPath(path), bufferSize);
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
    // we can do FS ops with only two elevated permissions:
    // 1) hadoop dynamic proxy is messy with access rules
    // 2) allow hadoop to add credentials to our Subject
    <V> V execute(Operation<V> operation) throws IOException {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }
        if (closed) {
            throw new AlreadyClosedException("HdfsBlobStore is closed: " + this);
        }
        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<V>() {
                @Override
                public V run() throws IOException {
                    return operation.run(fileContext);
                }
            }, null, new ReflectPermission("suppressAccessChecks"),
                     new AuthPermission("modifyPrivateCredentials"));
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getException();
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}