/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class LongObjectPagedHashMapTests extends ESTestCase {

    private BigArrays mockBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    public void testDuel() {
        final Map<Long, Object> map1 = new HashMap<>();
        final LongObjectPagedHashMap<Object> map2 = new LongObjectPagedHashMap<>(
            randomInt(42),
            0.6f + randomFloat() * 0.39f,
            mockBigArrays()
        );
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
        for (long i = 0; i <= maxKey; ++i) {
            assertSame(map1.get(i), map2.get(i));
        }
        final Map<Long, Object> copy = new HashMap<>();
        for (LongObjectPagedHashMap.Cursor<Object> cursor : map2) {
            copy.put(cursor.key, cursor.value);
        }
        map2.close();
        assertEquals(map1, copy);
    }

    public void testAllocation() {
        MockBigArrays.assertFitsIn(ByteSizeValue.ofBytes(256), bigArrays -> new LongObjectPagedHashMap<Object>(1, bigArrays));
    }

}
