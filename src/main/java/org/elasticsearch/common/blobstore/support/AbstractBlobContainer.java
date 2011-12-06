/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common.blobstore.support;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public abstract class AbstractBlobContainer implements BlobContainer {

    private final BlobPath path;

    protected AbstractBlobContainer(BlobPath path) {
        this.path = path;
    }

    @Override
    public BlobPath path() {
        return this.path;
    }

    @Override
    public byte[] readBlobFully(String blobName) throws IOException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        readBlob(blobName, new ReadBlobListener() {
            @Override
            public void onPartial(byte[] data, int offset, int size) {
                bos.write(data, offset, size);
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                failure.set(t);
                latch.countDown();
            }
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new InterruptedIOException("Interrupted while waiting to read [" + blobName + "]");
        }

        if (failure.get() != null) {
            if (failure.get() instanceof IOException) {
                throw (IOException) failure.get();
            } else {
                throw new IOException("Failed to get [" + blobName + "]", failure.get());
            }
        }
        return bos.toByteArray();
    }

    @Override
    public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        ImmutableMap<String, BlobMetaData> allBlobs = listBlobs();
        ImmutableMap.Builder<String, BlobMetaData> blobs = ImmutableMap.builder();
        for (BlobMetaData blob : allBlobs.values()) {
            if (blob.name().startsWith(blobNamePrefix)) {
                blobs.put(blob.name(), blob);
            }
        }
        return blobs.build();
    }

    @Override
    public void deleteBlobsByPrefix(final String blobNamePrefix) throws IOException {
        deleteBlobsByFilter(new BlobNameFilter() {
            @Override
            public boolean accept(String blobName) {
                return blobName.startsWith(blobNamePrefix);
            }
        });
    }

    @Override
    public void deleteBlobsByFilter(BlobNameFilter filter) throws IOException {
        ImmutableMap<String, BlobMetaData> blobs = listBlobs();
        for (BlobMetaData blob : blobs.values()) {
            if (filter.accept(blob.name())) {
                deleteBlob(blob.name());
            }
        }
    }
}
