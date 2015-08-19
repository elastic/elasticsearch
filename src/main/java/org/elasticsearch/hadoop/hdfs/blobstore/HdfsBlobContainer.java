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
package org.elasticsearch.hadoop.hdfs.blobstore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;

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
            return blobStore.fileSystemFactory().getFileSystem().exists(new Path(path, blobName));
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        blobStore.fileSystemFactory().getFileSystem().delete(new Path(path, blobName), true);
    }

    @Override
    public void move(String sourceBlobName, String targetBlobName) throws IOException {
        if (!blobStore.fileSystemFactory().getFileSystem().rename(new Path(path, sourceBlobName), new Path(path, targetBlobName))) {
            throw new IOException(String.format(Locale.ROOT, "can not move blob from [%s] to [%s]", sourceBlobName, targetBlobName));
        }
    }

    @Override
    public InputStream openInput(String blobName) throws IOException {
        // FSDataInputStream does buffering internally
        return blobStore.fileSystemFactory().getFileSystem().open(new Path(path, blobName), blobStore.bufferSizeInBytes());
    }

    @Override
    public OutputStream createOutput(String blobName) throws IOException {
        Path file = new Path(path, blobName);
        // FSDataOutputStream does buffering internally
        return blobStore.fileSystemFactory().getFileSystem().create(file, true, blobStore.bufferSizeInBytes());
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(final @Nullable String blobNamePrefix) throws IOException {
        FileStatus[] files = blobStore.fileSystemFactory().getFileSystem().listStatus(path, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(blobNamePrefix);
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
        FileStatus[] files = blobStore.fileSystemFactory().getFileSystem().listStatus(path);
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