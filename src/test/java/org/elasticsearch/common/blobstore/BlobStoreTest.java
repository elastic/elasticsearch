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

import com.carrotsearch.randomizedtesting.LifecycleScope;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;

public class BlobStoreTest extends ElasticsearchTestCase {

    @Test
    public void testWriteRead() throws IOException {
        final BlobStore store = newBlobStore();
        final BlobContainer container = store.blobContainer(new BlobPath());
        int length = randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16));
        byte[] data = new byte[length];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) randomInt();
        }
        try (OutputStream stream = container.createOutput("foobar")) {
            stream.write(data);
        }
        try (InputStream stream = container.openInput("foobar")) {
            BytesRef target = new BytesRef();
            while (target.length < data.length) {
                byte[] buffer = new byte[scaledRandomIntBetween(1, data.length - target.length)];
                int offset = scaledRandomIntBetween(0, buffer.length - 1);
                int read = stream.read(buffer, offset, buffer.length - offset);
                target.append(new BytesRef(buffer, offset, read));
            }
            assertEquals(data.length, target.length);
            assertArrayEquals(data, Arrays.copyOfRange(target.bytes, target.offset, target.length));
        }
        store.close();
    }

    protected BlobStore newBlobStore() {
        File tempDir = newTempDir(LifecycleScope.TEST);
        Settings settings = randomBoolean() ? ImmutableSettings.EMPTY : ImmutableSettings.builder().put("buffer_size", new ByteSizeValue(randomIntBetween(1, 100), ByteSizeUnit.KB)).build();
        FsBlobStore store = new FsBlobStore(settings, tempDir);
        return store;
    }
}
