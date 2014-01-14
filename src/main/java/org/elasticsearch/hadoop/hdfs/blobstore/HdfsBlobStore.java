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
import java.util.concurrent.Executor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

public class HdfsBlobStore extends AbstractComponent implements BlobStore {

    private final FileSystem fs;
    private final Path rootHdfsPath;
    private final Executor executor;
    private final int bufferSizeInBytes;

    public HdfsBlobStore(Settings settings, FileSystem fs, Path path, Executor executor) throws IOException {
        super(settings);
        this.fs = fs;
        this.rootHdfsPath = path;
        this.executor = executor;

        this.bufferSizeInBytes = (int) settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();

        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
    }

    @Override
    public String toString() {
        return rootHdfsPath.toUri().toString();
    }

    public FileSystem fileSystem() {
        return fs;
    }

    public Path path() {
        return rootHdfsPath;
    }

    public Executor executor() {
        return executor;
    }

    public int bufferSizeInBytes() {
        return bufferSizeInBytes;
    }

    @Override
    public ImmutableBlobContainer immutableBlobContainer(BlobPath path) {
        return new HdfsImmutableBlobContainer(path, this, buildHdfsPath(path));
    }

    @Override
    public void delete(BlobPath path) {
        try {
            fs.delete(translateToHdfsPath(path), true);
        } catch (IOException ex) {
        }
    }

    private Path buildHdfsPath(BlobPath blobPath) {
        Path path = translateToHdfsPath(blobPath);
        try {
            fs.mkdirs(path);
        } catch (IOException e) {
            // ignore
        }
        return path;
    }

    private Path translateToHdfsPath(BlobPath blobPath) {
        Path path = path();
        for (String p : blobPath) {
            path = new Path(path, p);
        }
        return path;
    }

    @Override
    public void close() {
        //
    }
}