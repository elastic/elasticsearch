/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ObjectObjectPagedHashMapTests extends ESTestCase {

    private BigArrays mockBigArrays(CircuitBreakerService service) {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), service, true);
    }

    public void testDuel() {
        // first with cranky
        try {
            doTestDuel(mockBigArrays(new CrankyCircuitBreakerService()));
        } catch (CircuitBreakingException ex) {
            assertThat(ex.getMessage(), equalTo("cranky breaker"));
        }
        // then to the end
        doTestDuel(mockBigArrays(new NoneCircuitBreakerService()));
    }

    private void doTestDuel(BigArrays bigArrays) {
        final Map<BytesRef, Object> map1 = new HashMap<>();
        try (
            ObjectObjectPagedHashMap<BytesRef, Object> map2 = new ObjectObjectPagedHashMap<>(
                randomInt(42),
                0.6f + randomFloat() * 0.39f,
                bigArrays
            )
        ) {
            final int maxKey = randomIntBetween(1, 10000);
            BytesRef[] bytesRefs = new BytesRef[maxKey];
            for (int i = 0; i < maxKey; i++) {
                bytesRefs[i] = randomBytesRef();
            }
            final int iters = scaledRandomIntBetween(10000, 100000);
            for (int i = 0; i < iters; ++i) {
                final boolean put = randomBoolean();
                final int iters2 = randomIntBetween(1, 100);
                for (int j = 0; j < iters2; ++j) {
                    final BytesRef key = bytesRefs[random().nextInt(maxKey)];
                    if (put) {
                        final Object value = new Object();
                        assertSame(map1.put(key, value), map2.put(key, value));
                    } else {
                        assertSame(map1.remove(key), map2.remove(key));
                    }
                    assertEquals(map1.size(), map2.size());
                }
            }
            for (int i = 0; i < maxKey; i++) {
                assertSame(map1.get(bytesRefs[i]), map2.get(bytesRefs[i]));
            }
            final Map<BytesRef, Object> copy = new HashMap<>();
            for (ObjectObjectPagedHashMap.Cursor<BytesRef, Object> cursor : map2) {
                copy.put(cursor.key, cursor.value);
            }
            assertEquals(map1, copy);
        }
    }

    private BytesRef randomBytesRef() {
        byte[] bytes = new byte[randomIntBetween(2, 20)];
        random().nextBytes(bytes);
        return new BytesRef(bytes);
    }

    public void testAllocation() {
        MockBigArrays.assertFitsIn(ByteSizeValue.ofBytes(256), bigArrays -> new ObjectObjectPagedHashMap<>(1, bigArrays));
    }

}
