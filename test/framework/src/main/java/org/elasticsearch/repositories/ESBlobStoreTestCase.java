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
package org.elasticsearch.repositories;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Generic test case for blob store implementation.
 * These tests check basic blob store functionality.
 */
public abstract class ESBlobStoreTestCase extends ESTestCase {

    public void testContainerCreationAndDeletion() throws IOException {
        try(final BlobStore store = newBlobStore()) {
            final BlobContainer containerFoo = store.blobContainer(new BlobPath().add("foo"));
            final BlobContainer containerBar = store.blobContainer(new BlobPath().add("bar"));
            byte[] data1 = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            byte[] data2 = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            writeBlob(containerFoo, "test", new BytesArray(data1));
            writeBlob(containerBar, "test", new BytesArray(data2));

            assertArrayEquals(readBlobFully(containerFoo, "test", data1.length), data1);
            assertArrayEquals(readBlobFully(containerBar, "test", data2.length), data2);

            assertTrue(containerFoo.blobExists("test"));
            assertTrue(containerBar.blobExists("test"));
            store.delete(new BlobPath());
            assertFalse(containerFoo.blobExists("test"));
            assertFalse(containerBar.blobExists("test"));
        }
    }

    public static byte[] writeRandomBlob(BlobContainer container, String name, int length) throws IOException {
        byte[] data = randomBytes(length);
        writeBlob(container, name, new BytesArray(data));
        return data;
    }

    public static byte[] readBlobFully(BlobContainer container, String name, int length) throws IOException {
        byte[] data = new byte[length];
        try (InputStream inputStream = container.readBlob(name)) {
            assertThat(inputStream.read(data), equalTo(length));
            assertThat(inputStream.read(), equalTo(-1));
        }
        return data;
    }

    public static byte[] randomBytes(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) randomInt();
        }
        return data;
    }

    private static void writeBlob(BlobContainer container, String blobName, BytesArray bytesArray) throws IOException {
        try (InputStream stream = bytesArray.streamInput()) {
            container.writeBlob(blobName, stream, bytesArray.length());
        }
    }

    protected abstract BlobStore newBlobStore() throws IOException;
}
