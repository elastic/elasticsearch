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

import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.LongLongOpenHashMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.elasticsearch.common.util.BigArraysTests;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.Iterator;

public class LongHashTests extends ElasticsearchTestCase {

    public void testDuell() {
        final Long[] values = new Long[randomIntBetween(1, 100000)];
        for (int i = 0; i < values.length; ++i) {
            values[i] = randomLong();
        }
        final LongLongMap valueToId = new LongLongOpenHashMap();
        final long[] idToValue = new long[values.length];
        // Test high load factors to make sure that collision resolution works fine
        final float maxLoadFactor = 0.6f + randomFloat() * 0.39f;
        final LongHash longHash = new LongHash(randomIntBetween(0, 100), maxLoadFactor, BigArraysTests.randombigArrays());
        final int iters = randomInt(1000000);
        for (int i = 0; i < iters; ++i) {
            final Long value = randomFrom(values);
            if (valueToId.containsKey(value)) {
                assertEquals(- 1 - valueToId.get(value), longHash.add(value));
            } else {
                assertEquals(valueToId.size(), longHash.add(value));
                idToValue[valueToId.size()] = value;
                valueToId.put(value, valueToId.size());
            }
        }

        assertEquals(valueToId.size(), longHash.size());
        for (Iterator<LongLongCursor> iterator = valueToId.iterator(); iterator.hasNext(); ) {
            final LongLongCursor next = iterator.next();
            assertEquals(next.value, longHash.find(next.key));
        }

        for (long i = 0; i < longHash.capacity(); ++i) {
            final long id = longHash.id(i);
            if (id >= 0) {
                assertEquals(idToValue[(int) id], longHash.key(i));
            }
        }
        longHash.release();
    }

}
