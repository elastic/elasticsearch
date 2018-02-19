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
package org.elasticsearch.percolator;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import java.util.stream.IntStream;

public class PercolatorMatchedSlotSubFetchPhaseTests extends ESTestCase {

    public void testConvertTopDocsToSlots() {
        ScoreDoc[] scoreDocs = new ScoreDoc[randomInt(128)];
        for (int i = 0; i < scoreDocs.length; i++) {
            scoreDocs[i] = new ScoreDoc(i, 1f);
        }

        TopDocs topDocs = new TopDocs(scoreDocs.length, scoreDocs, 1f);
        IntStream stream = PercolatorMatchedSlotSubFetchPhase.convertTopDocsToSlots(topDocs, null);

        int[] result = stream.toArray();
        assertEquals(scoreDocs.length, result.length);
        for (int i = 0; i < scoreDocs.length; i++) {
            assertEquals(scoreDocs[i].doc, result[i]);
        }
    }

    public void testConvertTopDocsToSlots_nestedDocs() {
        ScoreDoc[] scoreDocs = new ScoreDoc[5];
        scoreDocs[0] = new ScoreDoc(2, 1f);
        scoreDocs[1] = new ScoreDoc(5, 1f);
        scoreDocs[2] = new ScoreDoc(8, 1f);
        scoreDocs[3] = new ScoreDoc(11, 1f);
        scoreDocs[4] = new ScoreDoc(14, 1f);
        TopDocs topDocs = new TopDocs(scoreDocs.length, scoreDocs, 1f);

        FixedBitSet bitSet = new FixedBitSet(15);
        bitSet.set(2);
        bitSet.set(5);
        bitSet.set(8);
        bitSet.set(11);
        bitSet.set(14);

        int[] rootDocsBySlot = PercolatorMatchedSlotSubFetchPhase.buildRootDocsSlots(bitSet);
        int[] result = PercolatorMatchedSlotSubFetchPhase.convertTopDocsToSlots(topDocs, rootDocsBySlot).toArray();
        assertEquals(scoreDocs.length, result.length);
        assertEquals(0, result[0]);
        assertEquals(1, result[1]);
        assertEquals(2, result[2]);
        assertEquals(3, result[3]);
        assertEquals(4, result[4]);
    }

}
