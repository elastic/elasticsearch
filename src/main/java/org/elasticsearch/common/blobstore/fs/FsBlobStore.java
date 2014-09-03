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

package org.elasticsearch.common.blobstore.fs;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.File;

/**
 *
 */
public class FsBlobStore extends AbstractComponent implements BlobStore {

    private final File path;

    private final int bufferSizeInBytes;

    public FsBlobStore(Settings settings, File path) {
        super(settings);
        this.path = path;
        if (!path.exists()) {
            boolean b = FileSystemUtils.mkdirs(path);
            if (!b) {
                throw new BlobStoreException("Failed to create directory at [" + path + "]");
            }
        }
        if (!path.isDirectory()) {
            throw new BlobStoreException("Path is not a directory at [" + path + "]");
        }
        this.bufferSizeInBytes = (int) settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();
    }

    @Override
    public String toString() {
        return path.toString();
    }

    public File path() {
        return path;
    }

    public int bufferSizeInBytes() {
        return this.bufferSizeInBytes;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new FsBlobContainer(this, path, buildAndCreate(path));
    }

    @Override
    public void delete(BlobPath path) {
        FileSystemUtils.deleteRecursively(buildPath(path));
    }

    @Override
    public void close() {
        // nothing to do here...
    }

    private synchronized File buildAndCreate(BlobPath path) {
        File f = buildPath(path);
        FileSystemUtils.mkdirs(f);
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
