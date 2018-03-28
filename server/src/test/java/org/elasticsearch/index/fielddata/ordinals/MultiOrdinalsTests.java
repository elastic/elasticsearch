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
package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class MultiOrdinalsTests extends ESTestCase {

    protected Ordinals creationMultiOrdinals(OrdinalsBuilder builder) {
        return builder.build();
    }

    public void testRandomValues() throws IOException {
        Random random = random();
        int numDocs = 100 + random.nextInt(1000);
        int numOrdinals = 1 + random.nextInt(200);
        int numValues = 100 + random.nextInt(100000);
        OrdinalsBuilder builder = new OrdinalsBuilder(numDocs);
        Set<OrdAndId> ordsAndIdSet = new HashSet<>();
        for (int i = 0; i < numValues; i++) {
            ordsAndIdSet.add(new OrdAndId(random.nextInt(numOrdinals), random.nextInt(numDocs)));
        }
        List<OrdAndId> ordsAndIds = new ArrayList<>(ordsAndIdSet);
        Collections.sort(ordsAndIds, new Comparator<OrdAndId>() {

            @Override
            public int compare(OrdAndId o1, OrdAndId o2) {
                if (o1.ord < o2.ord) {
                    return -1;
                }
                if (o1.ord == o2.ord) {
                    if (o1.id < o2.id) {
                        return -1;
                    }
                    if (o1.id > o2.id) {
                        return 1;
                    }
                    return 0;
                }
                return 1;
            }
        });
        long lastOrd = -1;
        for (OrdAndId ordAndId : ordsAndIds) {
            if (lastOrd != ordAndId.ord) {
                lastOrd = ordAndId.ord;
                builder.nextOrdinal();
            }
            ordAndId.ord = builder.currentOrdinal(); // remap the ordinals in case we have gaps?
            builder.addDoc(ordAndId.id);
        }

        Collections.sort(ordsAndIds, new Comparator<OrdAndId>() {

            @Override
            public int compare(OrdAndId o1, OrdAndId o2) {
                if (o1.id < o2.id) {
                    return -1;
                }
                if (o1.id == o2.id) {
                    if (o1.ord < o2.ord) {
                        return -1;
                    }
                    if (o1.ord > o2.ord) {
                        return 1;
                    }
                    return 0;
                }
                return 1;
            }
        });
        Ordinals ords = creationMultiOrdinals(builder);
        SortedSetDocValues docs = ords.ordinals();
        final SortedDocValues singleOrds = MultiValueMode.MIN.select(docs);
        int docId = ordsAndIds.get(0).id;
        List<Long> docOrds = new ArrayList<>();
        for (OrdAndId ordAndId : ordsAndIds) {
            if (docId == ordAndId.id) {
                docOrds.add(ordAndId.ord);
            } else {
                if (!docOrds.isEmpty()) {
                    assertTrue(singleOrds.advanceExact(docId));
                    assertThat((long) singleOrds.ordValue(), equalTo(docOrds.get(0)));

                    assertTrue(docs.advanceExact(docId));
                    for (Long ord : docOrds) {
                        assertThat(docs.nextOrd(), equalTo(ord));
                    }
                    assertEquals(SortedSetDocValues.NO_MORE_ORDS, docs.nextOrd());
                }
                for (int i = docId + 1; i < ordAndId.id; i++) {
                    assertFalse(singleOrds.advanceExact(i));
                    assertFalse(docs.advanceExact(i));
                }
                docId = ordAndId.id;
                docOrds.clear();
                docOrds.add(ordAndId.ord);

            }
        }

    }

    public static class OrdAndId {
        long ord;
        final int id;

        public OrdAndId(long ord, int id) {
            this.ord = ord;
            this.id = id;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + id;
            result = prime * result + (int) ord;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            OrdAndId other = (OrdAndId) obj;
            if (id != other.id) {
                return false;
            }
            if (ord != other.ord) {
                return false;
            }
            return true;
        }
    }

    public void testOrdinals() throws Exception {
        int maxDoc = 7;
        long maxOrds = 32;
        OrdinalsBuilder builder = new OrdinalsBuilder(maxDoc);
        builder.nextOrdinal(); // 0
        builder.addDoc(1).addDoc(4).addDoc(5).addDoc(6);
        builder.nextOrdinal(); // 1
        builder.addDoc(0).addDoc(5).addDoc(6);
        builder.nextOrdinal(); // 3
        builder.addDoc(2).addDoc(4).addDoc(5).addDoc(6);
        builder.nextOrdinal(); // 3
        builder.addDoc(0).addDoc(4).addDoc(5).addDoc(6);
        builder.nextOrdinal(); // 4
        builder.addDoc(4).addDoc(5).addDoc(6);
        builder.nextOrdinal(); // 5
        builder.addDoc(4).addDoc(5).addDoc(6);
        while (builder.getValueCount() < maxOrds) {
            builder.nextOrdinal();
            builder.addDoc(5).addDoc(6);
        }

        long[][] ordinalPlan = new long[][]{
                {1, 3},
                {0},
                {2},
                {},
                {0, 2, 3, 4, 5},
                {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
                {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}
        };

        Ordinals ordinals = creationMultiOrdinals(builder);
        SortedSetDocValues docs = ordinals.ordinals();
        assertEquals(docs, ordinalPlan);
    }

    public void testMultiValuesDocsWithOverlappingStorageArrays() throws Exception {
        int maxDoc = 7;
        long maxOrds = 15;
        OrdinalsBuilder builder = new OrdinalsBuilder(maxDoc);
        for (int i = 0; i < maxOrds; i++) {
            builder.nextOrdinal();
            if (i < 10) {
                builder.addDoc(0);
            }
            builder.addDoc(1);
            if (i == 0) {
                builder.addDoc(2);
            }
            if (i < 5) {
                builder.addDoc(3);

            }
            if (i < 6) {
                builder.addDoc(4);

            }
            if (i == 1) {
                builder.addDoc(5);
            }
            if (i < 10) {
                builder.addDoc(6);
            }
        }

        long[][] ordinalPlan = new long[][]{
                {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                {0,1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
                {0},
                {0, 1, 2, 3, 4},
                {0, 1, 2, 3, 4, 5},
                {1},
                {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
        };

        Ordinals ordinals = new MultiOrdinals(builder, PackedInts.FASTEST);
        SortedSetDocValues docs = ordinals.ordinals();
        assertEquals(docs, ordinalPlan);
    }

    private void assertEquals(SortedSetDocValues docs, long[][] ordinalPlan) throws IOException {
        long maxOrd = 0;
        for (int doc = 0; doc < ordinalPlan.length; ++doc) {
            if (ordinalPlan[doc].length > 0) {
                maxOrd = Math.max(maxOrd, 1 + ordinalPlan[doc][ordinalPlan[doc].length - 1]);
            }
        }
        assertThat(docs.getValueCount(), equalTo(maxOrd));
        assertThat(FieldData.isMultiValued(docs), equalTo(true));
        for (int doc = 0; doc < ordinalPlan.length; ++doc) {
            long[] ords = ordinalPlan[doc];
            assertEquals(ords.length > 0, docs.advanceExact(doc));
            if (ords.length > 0) {
                for (long ord : ords) {
                    assertThat(docs.nextOrd(), equalTo(ord));
                }
                assertThat(docs.nextOrd(), equalTo(SortedSetDocValues.NO_MORE_ORDS));
            }
        }
    }

}
