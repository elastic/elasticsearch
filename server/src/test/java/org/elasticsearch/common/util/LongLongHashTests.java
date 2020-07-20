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

package org.elasticsearch.common.util;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class LongLongHashTests extends ESTestCase {
    private BigArrays randombigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private LongLongHash randomHash() {
        // Test high load factors to make sure that collision resolution works fine
        final float maxLoadFactor = 0.6f + randomFloat() * 0.39f;
        return new LongLongHash(randomIntBetween(0, 100), maxLoadFactor, randombigArrays());
    }

    public void testSimple() {
        try (LongLongHash hash = randomHash()) {
            assertThat(hash.add(0, 0), equalTo(0L));
            assertThat(hash.add(0, 1), equalTo(1L));
            assertThat(hash.add(0, 2), equalTo(2L));
            assertThat(hash.add(1, 0), equalTo(3L));
            assertThat(hash.add(1, 1), equalTo(4L));

            assertThat(hash.add(0, 0), equalTo(-1L));
            assertThat(hash.add(0, 2), equalTo(-3L));
            assertThat(hash.add(1, 1), equalTo(-5L));

            assertThat(hash.getKey1(0), equalTo(0L));
            assertThat(hash.getKey2(0), equalTo(0L));
            assertThat(hash.getKey1(4), equalTo(1L));
            assertThat(hash.getKey2(4), equalTo(1L));
        }
    }

    public void testDuel() {
        try (LongLongHash hash = randomHash()) {
            int iters = scaledRandomIntBetween(100, 100000);
            Key[] values = randomArray(1, iters, Key[]::new, () -> new Key(randomLong(), randomLong()));
            Map<Key, Integer> keyToId = new HashMap<>();
            List<Key> idToKey = new ArrayList<>();
            for (int i = 0; i < iters; ++i) {
                Key key = randomFrom(values);
                if (keyToId.containsKey(key)) {
                    assertEquals(-1 - keyToId.get(key), hash.add(key.key1, key.key2));
                } else {
                    assertEquals(keyToId.size(), hash.add(key.key1, key.key2));
                    keyToId.put(key, keyToId.size());
                    idToKey.add(key);
                }
            }

            assertEquals(keyToId.size(), hash.size());
            for (Map.Entry<Key, Integer> entry : keyToId.entrySet()) {
                assertEquals(entry.getValue().longValue(), hash.find(entry.getKey().key1, entry.getKey().key2));
            }

            assertEquals(idToKey.size(), hash.size());
            for (long i = 0; i < hash.capacity(); i++) {
                long id = hash.id(i);
                if (id >= 0) {
                    Key key = idToKey.get((int) id);
                    assertEquals(key.key1, hash.getKey1(id));
                    assertEquals(key.key2, hash.getKey2(id));
                }
            }

            for (long i = 0; i < hash.size(); i++) {
                Key key = idToKey.get((int) i);
                assertEquals(key.key1, hash.getKey1(i));
                assertEquals(key.key2, hash.getKey2(i));
            }
        }
    }

    class Key {
        long key1;
        long key2;

        Key(long key1, long key2) {
            this.key1 = key1;
            this.key2 = key2;
        }
    }

}
