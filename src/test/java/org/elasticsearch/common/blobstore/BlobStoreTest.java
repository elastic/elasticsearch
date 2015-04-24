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
package org.elasticsearch.common.blobstore;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class BlobStoreTest extends ElasticsearchTestCase {

    @Test
    public void testWriteRead() throws IOException {
        final BlobStore store = newBlobStore();
        final BlobContainer container = store.blobContainer(new BlobPath());
        byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
        try (OutputStream stream = container.createOutput("foobar")) {
            stream.write(data);
        }
        try (InputStream stream = container.openInput("foobar")) {
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
        store.close();
    }

    @Test
    public void testMoveAndList() throws IOException {
        final BlobStore store = newBlobStore();
        final BlobContainer container = store.blobContainer(new BlobPath());
        assertThat(container.listBlobs().size(), equalTo(0));
        int numberOfFooBlobs = randomIntBetween(0, 10);
        int numberOfBarBlobs = randomIntBetween(3, 20);
        Map<String, Long> generatedBlobs = newHashMap();
        for (int i = 0; i < numberOfFooBlobs; i++) {
            int length = randomIntBetween(10, 100);
            String name = "foo-" + i + "-";
            generatedBlobs.put(name, (long) length);
            createRandomBlob(container, name, length);
        }
        for (int i = 1; i < numberOfBarBlobs; i++) {
            int length = randomIntBetween(10, 100);
            String name = "bar-" + i + "-";
            generatedBlobs.put(name, (long) length);
            createRandomBlob(container, name, length);
        }
        int length = randomIntBetween(10, 100);
        String name = "bar-0-";
        generatedBlobs.put(name, (long) length);
        byte[] data = createRandomBlob(container, name, length);

        ImmutableMap<String, BlobMetaData> blobs = container.listBlobs();
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

        String newName = "bar-new";
        // Move to a new location
        container.move(name, newName);
        assertThat(container.listBlobsByPrefix(name).size(), equalTo(0));
        blobs = container.listBlobsByPrefix(newName);
        assertThat(blobs.size(), equalTo(1));
        assertThat(blobs.get(newName).length(), equalTo(generatedBlobs.get(name)));
        assertThat(data, equalTo(readBlobFully(container, newName, length)));
        store.close();
    }

    protected byte[] createRandomBlob(BlobContainer container, String name, int length) throws IOException {
        byte[] data = randomBytes(length);
        try (OutputStream stream = container.createOutput(name)) {
            stream.write(data);
        }
        return data;
    }

    protected byte[] readBlobFully(BlobContainer container, String name, int length) throws IOException {
        byte[] data = new byte[length];
        try (InputStream inputStream = container.openInput(name)) {
            assertThat(inputStream.read(data), equalTo(length));
            assertThat(inputStream.read(), equalTo(-1));
        }
        return data;
    }

    protected byte[] randomBytes(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) randomInt();
        }
        return data;
    }

    protected BlobStore newBlobStore() throws IOException {
        Path tempDir = createTempDir();
        Settings settings = randomBoolean() ? ImmutableSettings.EMPTY : ImmutableSettings.builder().put("buffer_size", new ByteSizeValue(randomIntBetween(1, 100), ByteSizeUnit.KB)).build();
        FsBlobStore store = new FsBlobStore(settings, tempDir);
        return store;
    }
}
