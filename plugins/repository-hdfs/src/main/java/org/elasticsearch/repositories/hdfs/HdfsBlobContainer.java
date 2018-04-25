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

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.repositories.hdfs.HdfsBlobStore.Operation;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;

final class HdfsBlobContainer extends AbstractBlobContainer {
    private final HdfsBlobStore store;
    private final HdfsSecurityContext securityContext;
    private final Path path;
    private final int bufferSize;

    HdfsBlobContainer(BlobPath blobPath, HdfsBlobStore store, Path path, int bufferSize, HdfsSecurityContext hdfsSecurityContext) {
        super(blobPath);
        this.store = store;
        this.securityContext = hdfsSecurityContext;
        this.path = path;
        this.bufferSize = bufferSize;
    }

    @Override
    public boolean blobExists(String blobName) {
        try {
            return store.execute(fileContext -> fileContext.util().exists(new Path(path, blobName)));
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }

        store.execute(fileContext -> fileContext.delete(new Path(path, blobName), true));
    }

    @Override
    public void move(String sourceBlobName, String targetBlobName) throws IOException {
        store.execute((Operation<Void>) fileContext -> {
            fileContext.rename(new Path(path, sourceBlobName), new Path(path, targetBlobName));
            return null;
        });
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }
        // FSDataInputStream does buffering internally
        // FSDataInputStream can open connections on read() or skip() so we wrap in
        // HDFSPrivilegedInputSteam which will ensure that underlying methods will
        // be called with the proper privileges.
        return store.execute(fileContext ->
            new HDFSPrivilegedInputSteam(fileContext.open(new Path(path, blobName), bufferSize), securityContext)
        );
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
        if (blobExists(blobName)) {
            throw new FileAlreadyExistsException("blob [" + blobName + "] already exists, cannot overwrite");
        }
        store.execute((Operation<Void>) fileContext -> {
            Path blob = new Path(path, blobName);
            // we pass CREATE, which means it fails if a blob already exists.
            // NOTE: this behavior differs from FSBlobContainer, which passes TRUNCATE_EXISTING
            // that should be fixed there, no need to bring truncation into this, give the user an error.
            EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK);
            CreateOpts[] opts = {CreateOpts.bufferSize(bufferSize)};
            try (FSDataOutputStream stream = fileContext.create(blob, flags, opts)) {
                int bytesRead;
                byte[] buffer = new byte[bufferSize];
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    stream.write(buffer, 0, bytesRead);
                    //  For safety we also hsync each write as well, because of its docs:
                    //  SYNC_BLOCK - to force closed blocks to the disk device
                    // "In addition Syncable.hsync() should be called after each write,
                    //  if true synchronous behavior is required"
                    stream.hsync();
                }
            }
            return null;
        });
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(@Nullable final String prefix) throws IOException {
        FileStatus[] files = store.execute(fileContext -> (fileContext.util().listStatus(path,
            path -> prefix == null || path.getName().startsWith(prefix))));
        Map<String, BlobMetaData> map = new LinkedHashMap<String, BlobMetaData>();
        for (FileStatus file : files) {
            map.put(file.getPath().getName(), new PlainBlobMetaData(file.getPath().getName(), file.getLen()));
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    /**
     * Exists to wrap underlying InputStream methods that might make socket connections in
     * doPrivileged blocks. This is due to the way that hdfs client libraries might open
     * socket connections when you are reading from an InputStream.
     */
    private static class HDFSPrivilegedInputSteam extends FilterInputStream {

        private final HdfsSecurityContext securityContext;

        HDFSPrivilegedInputSteam(InputStream in, HdfsSecurityContext hdfsSecurityContext) {
            super(in);
            this.securityContext = hdfsSecurityContext;
        }

        public int read() throws IOException {
            return securityContext.doPrivilegedOrThrow(in::read);
        }

        public int read(byte b[]) throws IOException {
            return securityContext.doPrivilegedOrThrow(() -> in.read(b));
        }

        public int read(byte b[], int off, int len) throws IOException {
            return securityContext.doPrivilegedOrThrow(() -> in.read(b, off, len));
        }

        public long skip(long n) throws IOException {
            return securityContext.doPrivilegedOrThrow(() -> in.skip(n));
        }

        public int available() throws IOException {
            return securityContext.doPrivilegedOrThrow(() -> in.available());
        }

        public synchronized void reset() throws IOException {
            securityContext.doPrivilegedOrThrow(() -> {
                in.reset();
                return null;
            });
        }
    }
}
