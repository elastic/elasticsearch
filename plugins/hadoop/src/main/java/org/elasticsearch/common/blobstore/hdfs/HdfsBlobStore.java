/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.blobstore.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.common.blobstore.AppendableBlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.common.util.concurrent.EsExecutors.*;

/**
 * @author kimchy (shay.banon)
 */
public class HdfsBlobStore implements BlobStore {

    private final FileSystem fileSystem;

    private final Path path;

    private final int bufferSizeInBytes;

    private final ExecutorService executorService;

    public HdfsBlobStore(Settings settings, FileSystem fileSystem, Path path) throws IOException {
        this.fileSystem = fileSystem;
        this.path = path;

        if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path);
        }

        this.bufferSizeInBytes = (int) settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();
        executorService = Executors.newCachedThreadPool(daemonThreadFactory(settings, "hdfs_blobstore"));
    }

    @Override public String toString() {
        return path.toString();
    }

    public int bufferSizeInBytes() {
        return this.bufferSizeInBytes;
    }

    public FileSystem fileSystem() {
        return fileSystem;
    }

    public Path path() {
        return path;
    }

    public ExecutorService executorService() {
        return executorService;
    }

    @Override public ImmutableBlobContainer immutableBlobContainer(BlobPath path) {
        return new HdfsImmutableBlobContainer(this, path, buildAndCreate(path));
    }

    @Override public AppendableBlobContainer appendableBlobContainer(BlobPath path) {
        return new HdfsAppendableBlobContainer(this, path, buildAndCreate(path));
    }

    @Override public void delete(BlobPath path) {
        try {
            fileSystem.delete(buildPath(path), true);
        } catch (IOException e) {
            // ignore
        }
    }

    @Override public void close() {
    }

    private Path buildAndCreate(BlobPath blobPath) {
        Path path = buildPath(blobPath);
        try {
            fileSystem.mkdirs(path);
        } catch (IOException e) {
            // ignore
        }
        return path;
    }

    private Path buildPath(BlobPath blobPath) {
        Path path = this.path;
        for (String p : blobPath) {
            path = new Path(path, p);
        }
        return path;
    }
}
