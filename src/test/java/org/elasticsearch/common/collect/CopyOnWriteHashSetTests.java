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

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.HashSet;
import java.util.Set;

public class CopyOnWriteHashSetTests extends ElasticsearchTestCase {

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
            final int valueBits = randomIntBetween(1, 30);
            final int hashBits = randomInt(valueBits);
            // we compute the total number of ops based on the bits of the hash
            // since the test is much heavier when few bits are used for the hash
            final int numOps = randomInt(10 + hashBits * 100);

            Set<O> ref = new HashSet<>();
            CopyOnWriteHashSet<O> set = new CopyOnWriteHashSet<>();
            assertEquals(ref, set);
            final int hashBase = randomInt();
            for (int i = 0; i < numOps; ++i) {
                final int v = randomInt(1 << valueBits);
                final int h = (v & ((1 << hashBits) - 1)) ^ hashBase;
                O key = new O(v, h);

                Set<O> newRef = new HashSet<>(ref);
                final CopyOnWriteHashSet<O> newSet;

                if (randomBoolean()) {
                    // ADD
                    newRef.add(key);
                    newSet = set.copyAndAdd(key);
                } else {
                    // REMOVE
                    final boolean modified = newRef.remove(key);
                    newSet = set.copyAndRemove(key);
                    if (!modified) {
                        assertSame(set, newSet);
                    }
                }

                assertEquals(ref, set); // make sure that the old copy has not been modified
                assertEquals(newRef, newSet);
                assertEquals(newSet, newRef);
                assertEquals(ref.isEmpty(), set.isEmpty());
                assertEquals(newRef.isEmpty(), newSet.isEmpty());

                ref = newRef;
                set = newSet;
            }
            assertEquals(ref, CopyOnWriteHashSet.copyOf(ref));
            assertEquals(ImmutableSet.of(), CopyOnWriteHashSet.copyOf(ref).copyAndRemoveAll(ref));
        }
    }

    public void testUnsupportedAPIs() {
        try {
            new CopyOnWriteHashSet<>().add("a");
            fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            new CopyOnWriteHashSet<>().copyAndAdd("a").remove("a");
            fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    public void testUnsupportedValues() {
        try {
            new CopyOnWriteHashSet<>().copyAndAdd(null);
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {
            // expected
        }
    }

}
