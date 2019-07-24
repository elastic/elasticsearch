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

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.ESBlobStoreTestCase;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class FsBlobStoreTests extends ESBlobStoreTestCase {

    protected BlobStore newBlobStore() throws IOException {
        final Settings settings;
        if (randomBoolean()) {
            settings = Settings.builder().put("buffer_size", new ByteSizeValue(randomIntBetween(1, 100), ByteSizeUnit.KB)).build();
        } else {
            settings = Settings.EMPTY;
        }
        return new FsBlobStore(settings, createTempDir(), false);
    }

    public void testReadOnly() throws Exception {
        Path tempDir = createTempDir();
        Path path = tempDir.resolve("bar");

        try (FsBlobStore store = new FsBlobStore(Settings.EMPTY, path, true)) {
            assertFalse(Files.exists(path));
            BlobPath blobPath = BlobPath.cleanPath().add("foo");
            store.blobContainer(blobPath);
            Path storePath = store.path();
            for (String d : blobPath) {
                storePath = storePath.resolve(d);
            }
            assertFalse(Files.exists(storePath));
        }

        try (FsBlobStore store = new FsBlobStore(Settings.EMPTY, path, false)) {
            assertTrue(Files.exists(path));
            BlobPath blobPath = BlobPath.cleanPath().add("foo");
            BlobContainer container = store.blobContainer(blobPath);
            Path storePath = store.path();
            for (String d : blobPath) {
                storePath = storePath.resolve(d);
            }
            assertTrue(Files.exists(storePath));
            assertTrue(Files.isDirectory(storePath));

            byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            writeBlob(container, "test", new BytesArray(data));
            assertArrayEquals(readBlobFully(container, "test", data.length), data);
            assertTrue(BlobStoreTestUtil.blobExists(container, "test"));
        }
    }
}
