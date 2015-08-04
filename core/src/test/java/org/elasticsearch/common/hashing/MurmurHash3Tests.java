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

package org.elasticsearch.common.hashing;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

public class MurmurHash3Tests extends ESTestCase {

    public void testHash128() {
        final int iters = scaledRandomIntBetween(100, 5000);
        for (int i = 0; i < iters; ++i) {
            final int seed = randomInt();
            final int offset = randomInt(20);
            final int len = randomInt(randomBoolean() ? 20 : 200);
            final byte[] bytes = new byte[len + offset + randomInt(3)];
            getRandom().nextBytes(bytes);
            HashCode h1 = Hashing.murmur3_128(seed).hashBytes(bytes, offset, len);
            MurmurHash3.Hash128 h2 = MurmurHash3.hash128(bytes, offset, len, seed, new MurmurHash3.Hash128());
            assertEquals(h1, h2);
        }
    }

    private void assertEquals(HashCode h1, MurmurHash3.Hash128 h2) {
        final LongBuffer longs = ByteBuffer.wrap(h1.asBytes()).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        assertEquals(2, longs.limit());
        assertEquals(h1.asLong(), h2.h1);
        assertEquals(longs.get(), h2.h1);
        assertEquals(longs.get(), h2.h2);
    }

}
