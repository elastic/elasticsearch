/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.elasticsearch.test.unit.index.fielddata.ordinals;

import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public abstract class MultiOrdinalsTests {

    protected abstract Ordinals creationMultiOrdinals(int[][] ordinals, int maxOrds);

    @Test
    public void testOrdinals() throws Exception {
        int maxDoc = 7;
        int maxOrds = 32;
        int[][] ords = new int[maxOrds][maxDoc];
        // Doc 1
        ords[0][0] = 2;
        ords[1][0] = 4;

        // Doc 2
        ords[0][1] = 1;

        // Doc 3
        ords[0][2] = 3;

        // Doc 4

        // Doc 5
        ords[0][4] = 1;
        ords[1][4] = 3;
        ords[2][4] = 4;
        ords[3][4] = 5;
        ords[4][4] = 6;

        // Doc 6
        for (int i = 0; i < maxOrds; i++) {
            ords[i][5] = (i + 1);
        }

        // Doc 7
        for (int i = 0; i < maxOrds; i++) {
            ords[i][6] = (i + 1);
        }

        Ordinals ordinals = creationMultiOrdinals(ords, maxOrds);
        Ordinals.Docs docs = ordinals.ordinals();
        assertThat(docs.getNumDocs(), equalTo(maxDoc));
        assertThat(docs.getNumOrds(), equalTo(maxOrds)); // Includes null ord
        assertThat(docs.isMultiValued(), equalTo(true));

        // Document 1
        assertThat(docs.getOrd(0), equalTo(2));
        IntArrayRef ref = docs.getOrds(0);
        assertThat(ref.start, equalTo(0));
        assertThat(ref.values[0], equalTo(2));
        assertThat(ref.values[1], equalTo(4));
        assertThat(ref.end, equalTo(2));
        assertIter(docs.getIter(0), 2, 4);
        docs.forEachOrdinalInDoc(0, assertOrdinalInProcDoc(2, 4));

        // Document 2
        assertThat(docs.getOrd(1), equalTo(1));
        ref = docs.getOrds(1);
        assertThat(ref.start, equalTo(0));
        assertThat(ref.values[0], equalTo(1));
        assertThat(ref.end, equalTo(1));
        assertIter(docs.getIter(1), 1);
        docs.forEachOrdinalInDoc(1, assertOrdinalInProcDoc(1));

        // Document 3
        assertThat(docs.getOrd(2), equalTo(3));
        ref = docs.getOrds(2);
        assertThat(ref.start, equalTo(0));
        assertThat(ref.values[0], equalTo(3));
        assertThat(ref.end, equalTo(1));
        assertIter(docs.getIter(2), 3);
        docs.forEachOrdinalInDoc(2, assertOrdinalInProcDoc(3));

        // Document 4
        assertThat(docs.getOrd(3), equalTo(0));
        ref = docs.getOrds(3);
        assertThat(ref.start, equalTo(0));
        assertThat(ref.end, equalTo(0));
        assertIter(docs.getIter(3));
        docs.forEachOrdinalInDoc(3, assertOrdinalInProcDoc(0));

        // Document 5
        assertThat(docs.getOrd(4), equalTo(1));
        ref = docs.getOrds(4);
        assertThat(ref.start, equalTo(0));
        assertThat(ref.values[0], equalTo(1));
        assertThat(ref.values[1], equalTo(3));
        assertThat(ref.values[2], equalTo(4));
        assertThat(ref.values[3], equalTo(5));
        assertThat(ref.values[4], equalTo(6));
        assertThat(ref.end, equalTo(5));
        assertIter(docs.getIter(4), 1, 3, 4, 5, 6);
        docs.forEachOrdinalInDoc(4, assertOrdinalInProcDoc(1, 3, 4, 5, 6));

        // Document 6
        assertThat(docs.getOrd(5), equalTo(1));
        ref = docs.getOrds(5);
        assertThat(ref.start, equalTo(0));
        int[] expectedOrds = new int[maxOrds];
        for (int i = 0; i < maxOrds; i++) {
            expectedOrds[i] = i + 1;
            assertThat(ref.values[i], equalTo(i + 1));
        }
        assertIter(docs.getIter(5), expectedOrds);
        docs.forEachOrdinalInDoc(5, assertOrdinalInProcDoc(expectedOrds));
        assertThat(ref.end, equalTo(maxOrds));

        // Document 7
        assertThat(docs.getOrd(6), equalTo(1));
        ref = docs.getOrds(6);
        assertThat(ref.start, equalTo(0));
        expectedOrds = new int[maxOrds];
        for (int i = 0; i < maxOrds; i++) {
            expectedOrds[i] = i + 1;
            assertThat(ref.values[i], equalTo(i + 1));
        }
        assertIter(docs.getIter(6), expectedOrds);
        docs.forEachOrdinalInDoc(6, assertOrdinalInProcDoc(expectedOrds));
        assertThat(ref.end, equalTo(maxOrds));
    }

    protected static void assertIter(Ordinals.Docs.Iter iter, int... expectedOrdinals) {
        for (int expectedOrdinal : expectedOrdinals) {
            assertThat(iter.next(), equalTo(expectedOrdinal));
        }
        assertThat(iter.next(), equalTo(0)); // Last one should always be 0
        assertThat(iter.next(), equalTo(0)); // Just checking it stays 0
    }

    protected static Ordinals.Docs.OrdinalInDocProc assertOrdinalInProcDoc(int... expectedOrdinals) {
        return new AssertingOrdinalInDocProc(expectedOrdinals);
    }

    static class AssertingOrdinalInDocProc implements Ordinals.Docs.OrdinalInDocProc {

        private final int[] expectedOrdinals;
        private int index = 0;

        AssertingOrdinalInDocProc(int... expectedOrdinals) {
            this.expectedOrdinals = expectedOrdinals;
        }

        @Override
        public void onOrdinal(int docId, int ordinal) {
            assertThat(ordinal, equalTo(expectedOrdinals[index++]));
        }
    }

}
