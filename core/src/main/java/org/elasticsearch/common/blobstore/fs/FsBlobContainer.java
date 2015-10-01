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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.Streams;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

/**
 *
 */
public class FsBlobContainer extends AbstractBlobContainer {

    protected final FsBlobStore blobStore;

    protected final Path path;

    public FsBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path) {
        super(blobPath);
        this.blobStore = blobStore;
        this.path = path;
    }

    @Override
    public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    @Override
    public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        // using MapBuilder and not ImmutableMap.Builder as it seems like File#listFiles might return duplicate files!
        MapBuilder<String, BlobMetaData> builder = MapBuilder.newMapBuilder();

        blobNamePrefix = blobNamePrefix == null ? "" : blobNamePrefix;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, blobNamePrefix + "*")) {
            for (Path file : stream) {
                final BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
                if (attrs.isRegularFile()) {
                    builder.put(file.getFileName().toString(), new PlainBlobMetaData(file.getFileName().toString(), attrs.size()));
                }
            }
        }
        return builder.immutableMap();
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        Path blobPath = path.resolve(blobName);
        Files.deleteIfExists(blobPath);
    }

    @Override
    public boolean blobExists(String blobName) {
        return Files.exists(path.resolve(blobName));
    }

    @Override
    public InputStream readBlob(String name) throws IOException {
        return new BufferedInputStream(Files.newInputStream(path.resolve(name)), blobStore.bufferSizeInBytes());
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
        final Path file = path.resolve(blobName);
        try (OutputStream outputStream = Files.newOutputStream(file)) {
            Streams.copy(inputStream, outputStream, new byte[blobStore.bufferSizeInBytes()]);
        }
        IOUtils.fsync(file, false);
        IOUtils.fsync(path, true);
    }

    @Override
    public void writeBlob(String blobName, BytesReference data) throws IOException {
        final Path file = path.resolve(blobName);
        try (OutputStream outputStream = Files.newOutputStream(file)) {
            data.writeTo(outputStream);
        }
        IOUtils.fsync(file, false);
        IOUtils.fsync(path, true);
    }

    @Override
    public void move(String source, String target) throws IOException {
        Path sourcePath = path.resolve(source);
        Path targetPath = path.resolve(target);
        // If the target file exists then Files.move() behaviour is implementation specific
        // the existing file might be replaced or this method fails by throwing an IOException.
        assert !Files.exists(targetPath);
        Files.move(sourcePath, targetPath, StandardCopyOption.ATOMIC_MOVE);
        IOUtils.fsync(path, true);
    }
}
