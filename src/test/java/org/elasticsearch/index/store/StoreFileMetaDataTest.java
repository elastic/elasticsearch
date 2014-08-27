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
package org.elasticsearch.index.store;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

public class StoreFileMetaDataTest extends ElasticsearchTestCase {

    public void testHashCodeAndEquals() throws IOException {
        final int iters = scaledRandomIntBetween(1, 10);
        for (int iter = 0; iter < iters; iter++) {
            final BytesRef hash = new BytesRef(scaledRandomIntBetween(0, 1024 * 1024));
            hash.length = hash.bytes.length;
            for (int i = 0; i < hash.length; i++) {
                hash.bytes[i] = randomByte();
            }
            final String name = randomRealisticUnicodeOfCodepointLengthBetween(1, 10);
            final long len = Math.max(0, Math.abs(randomLong()));
            final String checksum = randomAsciiOfLengthBetween(1, 10);
            final Version version = RandomPicks.randomFrom(getRandom(), Version.values());
            StoreFileMetaData meta = new StoreFileMetaData(name, len, checksum, version, BytesRef.deepCopyOf(hash));
            assertEquals(maybeSerialize(meta), new StoreFileMetaData(name, len, checksum, version, BytesRef.deepCopyOf(hash)));
            assertEquals(maybeSerialize(meta).hashCode(), new StoreFileMetaData(name, len, checksum, version, BytesRef.deepCopyOf(hash)).hashCode());
            assertThat(maybeSerialize(meta), not(equalTo(new StoreFileMetaData(name + "foobar", len, checksum, version, BytesRef.deepCopyOf(hash)))));
            assertThat(maybeSerialize(meta), not(equalTo(new StoreFileMetaData(name, len + 1, checksum, version, BytesRef.deepCopyOf(hash)))));
            assertThat(maybeSerialize(meta), not(equalTo(new StoreFileMetaData(name, len, checksum + "foo", version, BytesRef.deepCopyOf(hash)))));
            Version otherVersion = version;
            while (otherVersion == version) {
                otherVersion = RandomPicks.randomFrom(getRandom(), Version.values());
            }
            assertThat(maybeSerialize(meta), not(equalTo(new StoreFileMetaData(name, len, checksum, otherVersion, BytesRef.deepCopyOf(hash)))));
            assertThat(maybeSerialize(meta), not(equalTo(new StoreFileMetaData(name, len, checksum, version, null))));
            BytesRef otherHash = BytesRef.deepCopyOf(hash);
            while (otherHash.equals(hash)) {
                otherHash.copyChars(randomAsciiOfLengthBetween(1, 10));
            }
            assertThat(maybeSerialize(meta), not(equalTo(new StoreFileMetaData(name, len, checksum, version, otherHash))));
        }
    }

    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void testNullName() {
        new StoreFileMetaData(null, 1, "", TEST_VERSION_CURRENT, null);
    }

    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void testNegativeLen() {
        new StoreFileMetaData("", Math.min(-1, -randomInt()), "", TEST_VERSION_CURRENT, null);
    }

    public StoreFileMetaData maybeSerialize(StoreFileMetaData meta) throws IOException {
        if (randomBoolean()) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            meta.writeTo(new OutputStreamStreamOutput(outputStream));
            return StoreFileMetaData.readStoreFileMetaData(new InputStreamStreamInput(new ByteArrayInputStream(outputStream.toByteArray())));
        }
        return meta;
    }
}
