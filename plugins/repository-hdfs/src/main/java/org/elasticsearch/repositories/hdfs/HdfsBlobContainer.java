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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public class HdfsBlobContainer extends AbstractBlobContainer {

    protected final HdfsBlobStore blobStore;
    protected final Path path;

    public HdfsBlobContainer(BlobPath blobPath, HdfsBlobStore blobStore, Path path) {
        super(blobPath);
        this.blobStore = blobStore;
        this.path = path;
    }

    @Override
    public boolean blobExists(String blobName) {
        try {
            return SecurityUtils.execute(blobStore.fileSystemFactory(), new FsCallback<Boolean>() {
                @Override
                public Boolean doInHdfs(FileSystem fs) throws IOException {
                    return fs.exists(new Path(path, blobName));
                }
            });
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        SecurityUtils.execute(blobStore.fileSystemFactory(), new FsCallback<Boolean>() {
            @Override
            public Boolean doInHdfs(FileSystem fs) throws IOException {
                return fs.delete(new Path(path, blobName), true);
            }
        });
    }

    @Override
    public void move(String sourceBlobName, String targetBlobName) throws IOException {
        boolean rename = SecurityUtils.execute(blobStore.fileSystemFactory(), new FsCallback<Boolean>() {
            @Override
            public Boolean doInHdfs(FileSystem fs) throws IOException {
                return fs.rename(new Path(path, sourceBlobName), new Path(path, targetBlobName));
            }
        });
        
        if (!rename) {
            throw new IOException(String.format(Locale.ROOT, "can not move blob from [%s] to [%s]", sourceBlobName, targetBlobName));
        }
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        // FSDataInputStream does buffering internally
        return SecurityUtils.execute(blobStore.fileSystemFactory(), new FsCallback<InputStream>() {
            @Override
            public InputStream doInHdfs(FileSystem fs) throws IOException {
                return fs.open(new Path(path, blobName), blobStore.bufferSizeInBytes());
            }
        });
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
        SecurityUtils.execute(blobStore.fileSystemFactory(), new FsCallback<Void>() {
            @Override
            public Void doInHdfs(FileSystem fs) throws IOException {
                try (OutputStream stream = createOutput(blobName)) {
                    Streams.copy(inputStream, stream);
                }
                return null;
            }
        });
    }

    @Override
    public void writeBlob(String blobName, BytesReference bytes) throws IOException {
        SecurityUtils.execute(blobStore.fileSystemFactory(), new FsCallback<Void>() {
            @Override
            public Void doInHdfs(FileSystem fs) throws IOException {
                try (OutputStream stream = createOutput(blobName)) {
                    bytes.writeTo(stream);
                }
                return null;
            }
        });
    }
    
    private OutputStream createOutput(String blobName) throws IOException {
        Path file = new Path(path, blobName);
        // FSDataOutputStream does buffering internally
        return blobStore.fileSystemFactory().getFileSystem().create(file, true, blobStore.bufferSizeInBytes());
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(final @Nullable String blobNamePrefix) throws IOException {
        FileStatus[] files = SecurityUtils.execute(blobStore.fileSystemFactory(), new FsCallback<FileStatus[]>() {
            @Override
            public FileStatus[] doInHdfs(FileSystem fs) throws IOException {
                return fs.listStatus(path, new PathFilter() {
                    @Override
                    public boolean accept(Path path) {
                        return path.getName().startsWith(blobNamePrefix);
                    }
                });
            }
        });
        if (files == null || files.length == 0) {
            return Collections.emptyMap();
        }
        Map<String, BlobMetaData> map = new LinkedHashMap<String, BlobMetaData>();
        for (FileStatus file : files) {
            map.put(file.getPath().getName(), new PlainBlobMetaData(file.getPath().getName(), file.getLen()));
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        FileStatus[] files = SecurityUtils.execute(blobStore.fileSystemFactory(), new FsCallback<FileStatus[]>() {
            @Override
            public FileStatus[] doInHdfs(FileSystem fs) throws IOException {
                return fs.listStatus(path);
            }
        });
        if (files == null || files.length == 0) {
            return Collections.emptyMap();
        }
        Map<String, BlobMetaData> map = new LinkedHashMap<String, BlobMetaData>();
        for (FileStatus file : files) {
            map.put(file.getPath().getName(), new PlainBlobMetaData(file.getPath().getName(), file.getLen()));
        }
        return Collections.unmodifiableMap(map);
    }
}