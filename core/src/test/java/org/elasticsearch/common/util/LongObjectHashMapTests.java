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

import com.carrotsearch.hppc.LongObjectHashMap;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;

public class LongObjectHashMapTests extends ESSingleNodeTestCase {

    @Test
    public void duel() {
        final LongObjectHashMap<Object> map1 = new LongObjectHashMap<>();
        final LongObjectPagedHashMap<Object> map2 = new LongObjectPagedHashMap<>(randomInt(42), 0.6f + randomFloat() * 0.39f, BigArraysTests.randombigArrays());
        final int maxKey = randomIntBetween(1, 10000);
        final int iters = scaledRandomIntBetween(10000, 100000);
        for (int i = 0; i < iters; ++i) {
            final boolean put = randomBoolean();
            final int iters2 = randomIntBetween(1, 100);
            for (int j = 0; j < iters2; ++j) {
                final long key = randomInt(maxKey);
                if (put) {
                    final Object value = new Object();
                    assertSame(map1.put(key, value), map2.put(key, value));
                } else {
                    assertSame(map1.remove(key), map2.remove(key));
                }
                assertEquals(map1.size(), map2.size());
            }
        }
        for (int i = 0; i <= maxKey; ++i) {
            assertSame(map1.get(i), map2.get(i));
        }
        final LongObjectHashMap<Object> copy = new LongObjectHashMap<>();
        for (LongObjectPagedHashMap.Cursor<Object> cursor : map2) {
            copy.put(cursor.key, cursor.value);
        }
        map2.close();
        assertEquals(map1, copy);
    }

}
