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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.repositories.ESBlobStoreTestCase.randomBytes;
import static org.elasticsearch.repositories.ESBlobStoreTestCase.writeRandomBlob;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

/**
 * Generic test case for blob store container implementation.
 * These tests check basic blob store functionality.
 */
public abstract class ESBlobStoreContainerTestCase extends ESTestCase {

    public void testReadNonExistingPath() throws IOException {
        try(BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(new BlobPath());
            expectThrows(NoSuchFileException.class, () -> {
                try (InputStream is = container.readBlob("non-existing")) {
                    is.read();
                }
            });
        }
    }

    public void testWriteRead() throws IOException {
        try(BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(new BlobPath());
            byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            writeBlob(container, "foobar", new BytesArray(data), randomBoolean());
            if (randomBoolean()) {
                // override file, to check if we get latest contents
                data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
                writeBlob(container, "foobar", new BytesArray(data), false);
            }
            try (InputStream stream = container.readBlob("foobar")) {
                BytesRefBuilder target = new BytesRefBuilder();
                while (target.length() < data.length) {
                    byte[] buffer = new byte[scaledRandomIntBetween(1, data.length - target.length())];
                    int offset = scaledRandomIntBetween(0, buffer.length - 1);
                    int read = stream.read(buffer, offset, buffer.length - offset);
                    target.append(new BytesRef(buffer, offset, read));
                }
                assertEquals(data.length, target.length());
                assertArrayEquals(data, Arrays.copyOfRange(target.bytes(), 0, target.length()));
            }
        }
    }

    public void testList() throws IOException {
        try(BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(new BlobPath());
            assertThat(container.listBlobs().size(), equalTo(0));
            int numberOfFooBlobs = randomIntBetween(0, 10);
            int numberOfBarBlobs = randomIntBetween(3, 20);
            Map<String, Long> generatedBlobs = new HashMap<>();
            for (int i = 0; i < numberOfFooBlobs; i++) {
                int length = randomIntBetween(10, 100);
                String name = "foo-" + i + "-";
                generatedBlobs.put(name, (long) length);
                writeRandomBlob(container, name, length);
            }
            for (int i = 1; i < numberOfBarBlobs; i++) {
                int length = randomIntBetween(10, 100);
                String name = "bar-" + i + "-";
                generatedBlobs.put(name, (long) length);
                writeRandomBlob(container, name, length);
            }
            int length = randomIntBetween(10, 100);
            String name = "bar-0-";
            generatedBlobs.put(name, (long) length);
            writeRandomBlob(container, name, length);

            Map<String, BlobMetaData> blobs = container.listBlobs();
            assertThat(blobs.size(), equalTo(numberOfFooBlobs + numberOfBarBlobs));
            for (Map.Entry<String, Long> generated : generatedBlobs.entrySet()) {
                BlobMetaData blobMetaData = blobs.get(generated.getKey());
                assertThat(generated.getKey(), blobMetaData, notNullValue());
                assertThat(blobMetaData.name(), equalTo(generated.getKey()));
                assertThat(blobMetaData.length(), equalTo(generated.getValue()));
            }

            assertThat(container.listBlobsByPrefix("foo-").size(), equalTo(numberOfFooBlobs));
            assertThat(container.listBlobsByPrefix("bar-").size(), equalTo(numberOfBarBlobs));
            assertThat(container.listBlobsByPrefix("baz-").size(), equalTo(0));
        }
    }

    public void testDeleteBlob() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final String blobName = "foobar";
            final BlobContainer container = store.blobContainer(new BlobPath());
            expectThrows(NoSuchFileException.class, () -> container.deleteBlob(blobName));

            byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            final BytesArray bytesArray = new BytesArray(data);
            writeBlob(container, blobName, bytesArray, randomBoolean());
            container.deleteBlob(blobName); // should not raise

            // blob deleted, so should raise again
            expectThrows(NoSuchFileException.class, () -> container.deleteBlob(blobName));
        }
    }

    public void testDeleteBlobs() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final List<String> blobNames = Arrays.asList("foobar", "barfoo");
            final BlobContainer container = store.blobContainer(new BlobPath());
            container.deleteBlobsIgnoringIfNotExists(blobNames); // does not raise when blobs don't exist
            byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            final BytesArray bytesArray = new BytesArray(data);
            for (String blobName : blobNames) {
                writeBlob(container, blobName, bytesArray, randomBoolean());
            }
            assertEquals(container.listBlobs().size(), 2);
            container.deleteBlobsIgnoringIfNotExists(blobNames);
            assertTrue(container.listBlobs().isEmpty());
            container.deleteBlobsIgnoringIfNotExists(blobNames); // does not raise when blobs don't exist
        }
    }

    public void testDeleteBlobIgnoringIfNotExists() throws IOException {
        try (BlobStore store = newBlobStore()) {
            BlobPath blobPath = new BlobPath();
            if (randomBoolean()) {
                blobPath = blobPath.add(randomAlphaOfLengthBetween(1, 10));
            }

            final BlobContainer container = store.blobContainer(blobPath);
            container.deleteBlobIgnoringIfNotExists("does_not_exist");
        }
    }

    public void testVerifyOverwriteFails() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final String blobName = "foobar";
            final BlobContainer container = store.blobContainer(new BlobPath());
            byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            final BytesArray bytesArray = new BytesArray(data);
            writeBlob(container, blobName, bytesArray, true);
            // should not be able to overwrite existing blob
            expectThrows(FileAlreadyExistsException.class, () -> writeBlob(container, blobName, bytesArray, true));
            container.deleteBlob(blobName);
            writeBlob(container, blobName, bytesArray, true); // after deleting the previous blob, we should be able to write to it again
        }
    }

    protected void writeBlob(final BlobContainer container, final String blobName, final BytesArray bytesArray,
                             boolean failIfAlreadyExists) throws IOException {
        try (InputStream stream = bytesArray.streamInput()) {
            if (randomBoolean()) {
                container.writeBlob(blobName, stream, bytesArray.length(), failIfAlreadyExists);
            } else {
                container.writeBlobAtomic(blobName, stream, bytesArray.length(), failIfAlreadyExists);
            }
        }
    }

    protected abstract BlobStore newBlobStore() throws IOException;
}
