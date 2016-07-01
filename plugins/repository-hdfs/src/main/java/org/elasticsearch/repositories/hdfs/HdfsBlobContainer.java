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
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.repositories.hdfs.HdfsBlobStore.Operation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;

final class HdfsBlobContainer extends AbstractBlobContainer {
    private final HdfsBlobStore store;
    private final Path path;
    private final int bufferSize;

    HdfsBlobContainer(BlobPath blobPath, HdfsBlobStore store, Path path, int bufferSize) {
        super(blobPath);
        this.store = store;
        this.path = path;
        this.bufferSize = bufferSize;
    }

    @Override
    public boolean blobExists(String blobName) {
        try {
            return store.execute(new Operation<Boolean>() {
                @Override
                public Boolean run(FileContext fileContext) throws IOException {
                    return fileContext.util().exists(new Path(path, blobName));
                }
            });
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        try {
            store.execute(new Operation<Boolean>() {
                @Override
                public Boolean run(FileContext fileContext) throws IOException {
                    return fileContext.delete(new Path(path, blobName), true);
                }
            });
        } catch (FileNotFoundException ok) {
            // behaves like Files.deleteIfExists
        }
    }

    @Override
    public void move(String sourceBlobName, String targetBlobName) throws IOException {
        store.execute(new Operation<Void>() {
            @Override
            public Void run(FileContext fileContext) throws IOException {
                fileContext.rename(new Path(path, sourceBlobName), new Path(path, targetBlobName));
                return null;
            }
        });
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        // FSDataInputStream does buffering internally
        return store.execute(new Operation<InputStream>() {
            @Override
            public InputStream run(FileContext fileContext) throws IOException {
                return fileContext.open(new Path(path, blobName), bufferSize);
            }
        });
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
        store.execute(new Operation<Void>() {
            @Override
            public Void run(FileContext fileContext) throws IOException {
                Path blob = new Path(path, blobName);
                // we pass CREATE, which means it fails if a blob already exists.
                // NOTE: this behavior differs from FSBlobContainer, which passes TRUNCATE_EXISTING
                // that should be fixed there, no need to bring truncation into this, give the user an error.
                EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK);
                CreateOpts[] opts = { CreateOpts.bufferSize(bufferSize) };
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
            }
        });
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(final @Nullable String prefix) throws IOException {
        FileStatus[] files = store.execute(new Operation<FileStatus[]>() {
            @Override
            public FileStatus[] run(FileContext fileContext) throws IOException {
                return (fileContext.util().listStatus(path, new PathFilter() {
                    @Override
                    public boolean accept(Path path) {
                        return prefix == null || path.getName().startsWith(prefix);
                    }
                }));
            }
        });
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
}
