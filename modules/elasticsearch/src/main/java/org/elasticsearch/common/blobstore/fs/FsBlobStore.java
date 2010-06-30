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

package org.elasticsearch.common.blobstore.fs;

import org.elasticsearch.common.blobstore.*;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.util.concurrent.EsExecutors.*;

/**
 * @author kimchy (shay.banon)
 */
public class FsBlobStore implements BlobStore {

    private final File path;

    private final ExecutorService executorService;

    private final int bufferSizeInBytes;

    public FsBlobStore(Settings settings, File path) {
        this.path = path;
        if (!path.exists()) {
            boolean b = path.mkdirs();
            if (!b) {
                throw new BlobStoreException("Failed to create directory at [" + path + "]");
            }
        }
        if (!path.isDirectory()) {
            throw new BlobStoreException("Path is not a directory at [" + path + "]");
        }
        this.bufferSizeInBytes = (int) settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();
        this.executorService = Executors.newCachedThreadPool(daemonThreadFactory(settings, "fs_blobstore"));
    }

    @Override public String toString() {
        return path.toString();
    }

    public File path() {
        return path;
    }

    public int bufferSizeInBytes() {
        return this.bufferSizeInBytes;
    }

    public ExecutorService executorService() {
        return executorService;
    }

    @Override public ImmutableBlobContainer immutableBlobContainer(BlobPath path) {
        return new FsImmutableBlobContainer(this, path, buildAndCreate(path));
    }

    @Override public AppendableBlobContainer appendableBlobContainer(BlobPath path) {
        return new FsAppendableBlobContainer(this, path, buildAndCreate(path));
    }

    @Override public void delete(BlobPath path) {
        FileSystemUtils.deleteRecursively(buildPath(path));
    }

    @Override public void close() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        executorService.shutdownNow();
    }

    private synchronized File buildAndCreate(BlobPath path) {
        File f = buildPath(path);
        f.mkdirs();
        return f;
    }

    private File buildPath(BlobPath path) {
        String[] paths = path.toArray();
        if (paths.length == 0) {
            return path();
        }
        File blobPath = new File(this.path, paths[0]);
        if (paths.length > 1) {
            for (int i = 1; i < paths.length; i++) {
                blobPath = new File(blobPath, paths[i]);
            }
        }
        return blobPath;
    }
}
