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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;

final class HdfsBlobContainer extends AbstractBlobContainer {

    private final HdfsBlobStore blobStore;
    private final Path path;

    HdfsBlobContainer(BlobPath blobPath, HdfsBlobStore blobStore, Path path) {
        super(blobPath);
        this.blobStore = blobStore;
        this.path = path;
    }

    @Override
    public boolean blobExists(String blobName) {
        try {
            return SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<Boolean>() {
                @Override
                public Boolean doInHdfs(FileContext fc) throws IOException {
                    return fc.util().exists(new Path(path, blobName));
                }
            });
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        try {
            SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<Boolean>() {
                @Override
                public Boolean doInHdfs(FileContext fc) throws IOException {
                    return fc.delete(new Path(path, blobName), true);
                }
            });
        } catch (FileNotFoundException ok) {
            // behaves like Files.deleteIfExists
        }
    }

    @Override
    public void move(String sourceBlobName, String targetBlobName) throws IOException {
        SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<Void>() {
            @Override
            public Void doInHdfs(FileContext fc) throws IOException {
                fc.rename(new Path(path, sourceBlobName), new Path(path, targetBlobName));
                return null;
            }
        });
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        // FSDataInputStream does buffering internally
        return SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<InputStream>() {
            @Override
            public InputStream doInHdfs(FileContext fc) throws IOException {
                return fc.open(new Path(path, blobName), blobStore.bufferSizeInBytes());
            }
        });
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
        SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<Void>() {
            @Override
            public Void doInHdfs(FileContext fc) throws IOException {
                Path blob = new Path(path, blobName);
                // we pass CREATE, which means it fails if a blob already exists.
                // NOTE: this behavior differs from FSBlobContainer, which passes TRUNCATE_EXISTING
                // that should be fixed there, no need to bring truncation into this, give the user an error.
                EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK);
                CreateOpts[] opts = { CreateOpts.bufferSize(blobStore.bufferSizeInBytes()) };
                try (FSDataOutputStream stream = fc.create(blob, flags, opts)) {
                    int bytesRead;
                    byte[] buffer = new byte[blobStore.bufferSizeInBytes()];
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
    public Map<String, BlobMetaData> listBlobsByPrefix(final @Nullable String blobNamePrefix) throws IOException {
        FileStatus[] files = SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<FileStatus[]>() {
            @Override
            public FileStatus[] doInHdfs(FileContext fc) throws IOException {
                return (fc.util().listStatus(path, new PathFilter() {
                    @Override
                    public boolean accept(Path path) {
                        return path.getName().startsWith(blobNamePrefix);
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
        FileStatus[] files = SecurityUtils.execute(blobStore.fileContextFactory(), new FcCallback<FileStatus[]>() {
            @Override
            public FileStatus[] doInHdfs(FileContext fc) throws IOException {
                return fc.util().listStatus(path);
            }
        });
        Map<String, BlobMetaData> map = new LinkedHashMap<String, BlobMetaData>();
        for (FileStatus file : files) {
            map.put(file.getPath().getName(), new PlainBlobMetaData(file.getPath().getName(), file.getLen()));
        }
        return Collections.unmodifiableMap(map);
    }
}