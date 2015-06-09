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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.ImmutableMap;

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
    public boolean deleteBlob(String blobName) throws IOException {
        return blobStore.fileSystemFactory().getFileSystem().delete(new Path(path, blobName), true);
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
    public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(final @Nullable String blobNamePrefix) throws IOException {
        FileStatus[] files = blobStore.fileSystemFactory().getFileSystem().listStatus(path, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(blobNamePrefix);
            }
        });
        if (files == null || files.length == 0) {
            return ImmutableMap.of();
        }
        ImmutableMap.Builder<String, BlobMetaData> builder = ImmutableMap.builder();
        for (FileStatus file : files) {
            builder.put(file.getPath().getName(), new PlainBlobMetaData(file.getPath().getName(), file.getLen()));
        }
        return builder.build();
    }

    @Override
    public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
        FileStatus[] files = blobStore.fileSystemFactory().getFileSystem().listStatus(path);
        if (files == null || files.length == 0) {
            return ImmutableMap.of();
        }
        ImmutableMap.Builder<String, BlobMetaData> builder = ImmutableMap.builder();
        for (FileStatus file : files) {
            builder.put(file.getPath().getName(), new PlainBlobMetaData(file.getPath().getName(), file.getLen()));
        }
        return builder.build();
    }
}