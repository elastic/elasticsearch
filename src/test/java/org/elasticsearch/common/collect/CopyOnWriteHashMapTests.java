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

package org.elasticsearch.common.collect;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class CopyOnWriteHashMapTests extends ElasticsearchTestCase {

    private static class O {

        private final int value, hashCode;

        O(int value, int hashCode) {
            super();
            this.value = value;
            this.hashCode = hashCode;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof O)) {
                return false;
            }
            return value == ((O) obj).value;
        }
    }

    public void testDuel() {
        final int iters = scaledRandomIntBetween(2, 5);
        for (int iter = 0; iter < iters; ++iter) {
            final int numOps = randomInt(5000);
            final int valueBits = randomIntBetween(1, 30);
            final int hashBits = randomInt(valueBits);

            Map<O, Integer> ref = new HashMap<>();
            CopyOnWriteHashMap<O, Integer> map = new CopyOnWriteHashMap<>();
            assertEquals(ref, map);
            final int hashBase = randomInt();
            for (int i = 0; i < numOps; ++i) {
                final int v = randomInt(1 << valueBits);
                final int h = (v & ((1 << hashBits) - 1)) ^ hashBase;
                O key = new O(v, h);

                Map<O, Integer> newRef = new HashMap<>(ref);
                final CopyOnWriteHashMap<O, Integer> newMap;

                if (randomBoolean()) {
                    // ADD
                    Integer value = v;
                    newRef.put(key, value);
                    newMap = map.copyAndPut(key, value);
                } else {
                    // REMOVE
                    final Integer removed = newRef.remove(key);
                    newMap = map.copyAndRemove(key);
                    if (removed == null) {
                        assertSame(map, newMap);
                    }
                }

                assertEquals(ref, map); // make sure that the old copy has not been modified
                assertEquals(newRef, newMap);
                assertEquals(newMap, newRef);

                ref = newRef;
                map = newMap;
            }
            assertEquals(ref, CopyOnWriteHashMap.copyOf(ref));
            assertEquals(ImmutableMap.of(), CopyOnWriteHashMap.copyOf(ref).copyAndRemoveAll(ref.keySet()));
        }
    }

    public void testCollision() {
        CopyOnWriteHashMap<O, Integer> map = new CopyOnWriteHashMap<>();
        map = map.copyAndPut(new O(3, 0), 2);
        assertEquals((Integer) 2, map.get(new O(3, 0)));
        assertNull(map.get(new O(5, 0)));

        map = map.copyAndPut(new O(5, 0), 5);
        assertEquals((Integer) 2, map.get(new O(3, 0)));
        assertEquals((Integer) 5, map.get(new O(5, 0)));

        map = map.copyAndRemove(new O(3, 0));
        assertNull(map.get(new O(3, 0)));
        assertEquals((Integer) 5, map.get(new O(5, 0)));

        map = map.copyAndRemove(new O(5, 0));
        assertNull(map.get(new O(3, 0)));
        assertNull(map.get(new O(5, 0)));
    }

    public void testUnsupportedAPIs() {
        try {
            new CopyOnWriteHashMap<>().put("a", "b");
            fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            new CopyOnWriteHashMap<>().copyAndPut("a", "b").remove("a");
            fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    public void testUnsupportedValues() {
        try {
            new CopyOnWriteHashMap<>().copyAndPut("a", null);
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {
            // expected
        }
        
        try {
            new CopyOnWriteHashMap<>().copyAndPut(null, "b");
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {
            // expected
        }
    }

}
