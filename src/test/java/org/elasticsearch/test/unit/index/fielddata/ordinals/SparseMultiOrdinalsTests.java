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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.SparseMultiArrayOrdinals;
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.testng.Assert.fail;

/**
 */
public class SparseMultiOrdinalsTests extends MultiOrdinalsTests {

    @Override
    protected Ordinals creationMultiOrdinals(int[][] ordinals, int maxOrds) {
        return new SparseMultiArrayOrdinals(ordinals, maxOrds, 64);
    }

    @Test
    public void testMultiValuesSurpassOrdinalsLimit() throws Exception {
        int maxDoc = 2;
        int maxOrds = 128;
        int[][] ords = new int[maxOrds][maxDoc];
        // Doc 1
        ords[0][0] = 2;
        ords[1][0] = 4;

        // Doc 2
        for (int i = 0; i < maxOrds; i++) {
            ords[i][1] = (i + 1);
        }

        try {
            creationMultiOrdinals(ords, maxOrds);
            fail("Exception should have been throwed");
        } catch (ElasticSearchException e) {

        }
    }

    @Test
    public void testMultiValuesDocsWithOverlappingStorageArrays() throws Exception {
        int maxDoc = 7;
        int maxOrds = 15;
        int[][] ords = new int[maxOrds][maxDoc];
        // Doc 1
        for (int i = 0; i < 10; i++) {
            ords[i][0] = (i + 1);
        }

        // Doc 2
        for (int i = 0; i < 15; i++) {
            ords[i][1] = (i + 1);
        }

        // Doc 3
        ords[0][2] = 1;

        // Doc 4
        for (int i = 0; i < 5; i++) {
            ords[i][3] = (i + 1);
        }

        // Doc 5
        for (int i = 0; i < 6; i++) {
            ords[i][4] = (i + 1);
        }

        // Doc 6
        ords[0][5] = 2;

        // Doc 7
        for (int i = 0; i < 10; i++) {
            ords[i][6] = (i + 1);
        }

        Ordinals ordinals = new SparseMultiArrayOrdinals(ords, maxOrds, 20);
        Ordinals.Docs docs = ordinals.ordinals();
        assertThat(docs.getNumDocs(), equalTo(maxDoc));
        assertThat(docs.getNumOrds(), equalTo(maxOrds)); // Includes null ord
        assertThat(docs.isMultiValued(), equalTo(true));

        // Document 1
        assertThat(docs.getOrd(0), equalTo(1));
        IntArrayRef ref = docs.getOrds(0);
        assertThat(ref.start, equalTo(0));
        for (int i = 0; i < 10; i++) {
            assertThat(ref.values[i], equalTo(i + 1));
        }
        assertThat(ref.end, equalTo(10));

        // Document 2
        assertThat(docs.getOrd(1), equalTo(1));
        ref = docs.getOrds(1);
        assertThat(ref.start, equalTo(0));
        for (int i = 0; i < 15; i++) {
            assertThat(ref.values[i], equalTo(i + 1));
        }
        assertThat(ref.end, equalTo(15));

        // Document 3
        assertThat(docs.getOrd(2), equalTo(1));
        ref = docs.getOrds(2);
        assertThat(ref.start, equalTo(0));
        assertThat(ref.values[0], equalTo(1));
        assertThat(ref.end, equalTo(1));

        // Document 4
        assertThat(docs.getOrd(3), equalTo(1));
        ref = docs.getOrds(3);
        assertThat(ref.start, equalTo(0));
        for (int i = 0; i < 5; i++) {
            assertThat(ref.values[i], equalTo(i + 1));
        }
        assertThat(ref.end, equalTo(5));

        // Document 5
        assertThat(docs.getOrd(4), equalTo(1));
        ref = docs.getOrds(4);
        assertThat(ref.start, equalTo(0));
        for (int i = 0; i < 6; i++) {
            assertThat(ref.values[i], equalTo(i + 1));
        }
        assertThat(ref.end, equalTo(6));

        // Document 6
        assertThat(docs.getOrd(5), equalTo(2));
        ref = docs.getOrds(5);
        assertThat(ref.start, equalTo(0));
        assertThat(ref.values[0], equalTo(2));
        assertThat(ref.end, equalTo(1));

        // Document 7
        assertThat(docs.getOrd(6), equalTo(1));
        ref = docs.getOrds(6);
        assertThat(ref.start, equalTo(0));
        for (int i = 0; i < 10; i++) {
            assertThat(ref.values[i], equalTo(i + 1));
        }
        assertThat(ref.end, equalTo(10));
    }

}
