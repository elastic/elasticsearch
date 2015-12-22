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

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;

import java.io.IOException;

final class HdfsBlobStore implements BlobStore {

    private final HdfsRepository repository;
    private final Path root;

    HdfsBlobStore(HdfsRepository repository, Path root) throws IOException {
        this.repository = repository;
        this.root = root;

        try {
            mkdirs(root);
        } catch (FileAlreadyExistsException ok) {
            // behaves like Files.createDirectories
        }
    }

    private void mkdirs(Path path) throws IOException {
        repository.execute(new HdfsRepository.Operation<Void>() {
            @Override
            public Void run(FileContext fc) throws IOException {
                fc.mkdir(path, null, true);
                return null;
            }
        });
    }

    @Override
    public String toString() {
        return root.toUri().toString();
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new HdfsBlobContainer(path, repository, buildHdfsPath(path));
    }

    @Override
    public void delete(BlobPath path) throws IOException {
        repository.execute(new HdfsRepository.Operation<Void>() {
            @Override
            public Void run(FileContext fc) throws IOException {
                fc.delete(translateToHdfsPath(path), true);
                return null;
            }
        });
    }

    private Path buildHdfsPath(BlobPath blobPath) {
        final Path path = translateToHdfsPath(blobPath);
        try {
            mkdirs(path);
        } catch (FileAlreadyExistsException ok) {
            // behaves like Files.createDirectories
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to create blob container", ex);
        }
        return path;
    }

    private Path translateToHdfsPath(BlobPath blobPath) {
        Path path = root;
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