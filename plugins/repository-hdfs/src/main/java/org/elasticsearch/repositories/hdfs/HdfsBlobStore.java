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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.Executor;

public class HdfsBlobStore extends AbstractComponent implements BlobStore {

    private final FileSystemFactory ffs;
    private final Path rootHdfsPath;
    private final ThreadPool threadPool;
    private final int bufferSizeInBytes;

    public HdfsBlobStore(Settings settings, FileSystemFactory ffs, Path path, ThreadPool threadPool) throws IOException {
        super(settings);
        this.ffs = ffs;
        this.rootHdfsPath = path;
        this.threadPool = threadPool;

        this.bufferSizeInBytes = (int) settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();

        mkdirs(path);
    }

    private void mkdirs(Path path) throws IOException {
        SecurityUtils.execute(ffs, new FsCallback<Void>() {
            @Override
            public Void doInHdfs(FileSystem fs) throws IOException {
                if (!fs.exists(path)) {
                    fs.mkdirs(path);
                }
                return null;
            }
        });
    }

    @Override
    public String toString() {
        return rootHdfsPath.toUri().toString();
    }

    public FileSystemFactory fileSystemFactory() {
        return ffs;
    }

    public Path path() {
        return rootHdfsPath;
    }

    public Executor executor() {
        return threadPool.executor(ThreadPool.Names.SNAPSHOT);
    }

    public int bufferSizeInBytes() {
        return bufferSizeInBytes;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new HdfsBlobContainer(path, this, buildHdfsPath(path));
    }

    @Override
    public void delete(BlobPath path) throws IOException {
        SecurityUtils.execute(ffs, new FsCallback<Void>() {
            @Override
            public Void doInHdfs(FileSystem fs) throws IOException {
                fs.delete(translateToHdfsPath(path), true);
                return null;
            }
        });
    }

    private Path buildHdfsPath(BlobPath blobPath) {
        final Path path = translateToHdfsPath(blobPath);
        try {
            mkdirs(path);
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to create blob container", ex);
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