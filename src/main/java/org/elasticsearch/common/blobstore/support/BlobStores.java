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

package org.elasticsearch.common.blobstore.support;

import org.elasticsearch.common.blobstore.ImmutableBlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class BlobStores {

    public static void syncWriteBlob(ImmutableBlobContainer blobContainer, String blobName, InputStream is, long sizeInBytes) throws IOException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        blobContainer.writeBlob(blobName, is, sizeInBytes, new ImmutableBlobContainer.WriterListener() {
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
            throw new InterruptedIOException("Interrupted while waiting to write [" + blobName + "]");
        }

        if (failure.get() != null) {
            if (failure.get() instanceof IOException) {
                throw (IOException) failure.get();
            } else {
                throw new IOException("Failed to get [" + blobName + "]", failure.get());
            }
        }
    }
}
