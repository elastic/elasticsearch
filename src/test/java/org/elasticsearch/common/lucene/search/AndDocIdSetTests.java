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

package org.elasticsearch.common.lucene.search;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesDocIdSet;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.docset.AndDocIdSet;
import org.elasticsearch.test.ElasticsearchTestCase;

public class AndDocIdSetTests extends ElasticsearchTestCase {

    private static FixedBitSet randomBitSet(int numDocs) {
        FixedBitSet b = new FixedBitSet(numDocs);
        for (int i = 0; i < numDocs; ++i) {
            if (random().nextBoolean()) {
                b.set(i);
            }
        }
        return b;
    }

    public void testDuel() throws IOException {
        for (int iter = 0; iter < 1000; ++iter) {
            final int numSets = 1 + random().nextInt(5);
            final int numDocs = 1 + random().nextInt(1000);
            FixedBitSet anded = new FixedBitSet(numDocs);
            anded.set(0, numDocs);
            final DocIdSet[] sets = new DocIdSet[numSets];
            for (int i = 0; i < numSets; ++i) {
                final FixedBitSet randomSet = randomBitSet(numDocs);
                
                anded.and(randomSet);
                
                if (random().nextBoolean()) {
                    // will be considered 'fast' by AndDocIdSet
                    sets[i] = new BitDocIdSet(randomSet);
                } else {
                    // will be considered 'slow' by AndDocIdSet
                    sets[i] = new DocValuesDocIdSet(numDocs, null) {
                        @Override
                        protected boolean matchDoc(int doc) {
                            return randomSet.get(doc);
                        }
                    };
                }
            }
            AndDocIdSet andSet = new AndDocIdSet(sets);
            Bits andBits = andSet.bits();
            if (andBits != null) {
                for (int i = 0; i < numDocs; ++i) {
                    assertEquals(anded.get(i), andBits.get(i));
                }
            }
            DocIdSetIterator andIt = andSet.iterator();
            if (andIt == null) {
                assertEquals(0, anded.cardinality());
            } else {
                int previous = -1;
                for (int doc = andIt.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = andIt.nextDoc()) {
                    for (int j = previous + 1; j < doc; ++j) {
                        assertFalse(anded.get(j));
                    }
                    assertTrue(anded.get(doc));
                    previous = doc;
                }
                for (int j = previous + 1; j < numDocs; ++j) {
                    assertFalse(anded.get(j));
                }
            }
        }
    }

}
