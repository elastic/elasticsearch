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

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.index.mapper.KeyedJsonAtomicFieldData.KeyedJsonDocValues;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

public class KeyedJsonDocValuesTests extends ESTestCase {
    private SortedSetDocValues delegate;

    @Before
    public void setUpDelegate() {
        BytesRef[] allTerms = new BytesRef[60];
        long[] documentOrds = new long[50];
        int index = 0;

        for (int ord = 0; ord < allTerms.length; ord++) {
            String key;
            if (ord < 20) {
                key = "apple";
            } else if (ord < 30) {
                key = "avocado";
            } else if (ord < 40) {
                key = "banana";
            } else if (ord < 41) {
                key = "cantaloupe";
            } else {
                key = "cucumber";
            }

            allTerms[ord] = prefixedValue(key, "value" + ord);

            // Do not include the term 'avocado' in the mock document.
            if (key.equals("avocado") == false) {
                documentOrds[index++] = ord;
            }
        }

        delegate = new MockSortedSetDocValues(allTerms, documentOrds);
    }

    private BytesRef prefixedValue(String key, String value) {
        String term = JsonFieldParser.createKeyedValue(key, value);
        return new BytesRef(term);
    }

    public void testFindOrdinalBounds() throws IOException {
        testFindOrdinalBounds("apple", delegate, 0, 19);
        testFindOrdinalBounds("avocado", delegate, 20, 29);
        testFindOrdinalBounds("banana", delegate, 30, 39);
        testFindOrdinalBounds("berry", delegate, -1, -1);
        testFindOrdinalBounds("cantaloupe", delegate, 40, 40);
        testFindOrdinalBounds("cucumber", delegate, 41, 59);

        SortedSetDocValues emptyDelegate = new MockSortedSetDocValues(new BytesRef[0], new long[0]);
        testFindOrdinalBounds("apple", emptyDelegate, -1, -1);

        BytesRef[] terms = new BytesRef[] { prefixedValue("prefix", "value") };
        SortedSetDocValues singleValueDelegate = new MockSortedSetDocValues(terms, new long[0]);
        testFindOrdinalBounds("prefix", singleValueDelegate, 0, 0);
        testFindOrdinalBounds("prefix1", singleValueDelegate, -1, -1);

        terms = new BytesRef[] { prefixedValue("prefix", "value"),
            prefixedValue("prefix1", "value"),
            prefixedValue("prefix1", "value1"),
            prefixedValue("prefix2", "value"),
            prefixedValue("prefix3", "value")};
        SortedSetDocValues oddLengthDelegate = new MockSortedSetDocValues(terms, new long[0]);
        testFindOrdinalBounds("prefix", oddLengthDelegate, 0, 0);
        testFindOrdinalBounds("prefix1", oddLengthDelegate, 1, 2);
        testFindOrdinalBounds("prefix2", oddLengthDelegate, 3, 3);
        testFindOrdinalBounds("prefix3", oddLengthDelegate, 4, 4);
    }

    public void testFindOrdinalBounds(String key,
                                      SortedSetDocValues delegate,
                                      int minOrd, int maxOrd) throws IOException {
        BytesRef bytesKey = new BytesRef(key);
        assertEquals(minOrd, KeyedJsonDocValues.findMinOrd(bytesKey, delegate));
        assertEquals(maxOrd, KeyedJsonDocValues.findMaxOrd(bytesKey, delegate));
    }

    public void testAdvanceExact() throws IOException {
        KeyedJsonDocValues avocadoDocValues = KeyedJsonDocValues.create("avocado", delegate);
        assertFalse(avocadoDocValues.advanceExact(0));

        KeyedJsonDocValues bananaDocValues = KeyedJsonDocValues.create("banana", delegate);
        assertTrue(bananaDocValues.advanceExact(0));

        KeyedJsonDocValues nonexistentDocValues = KeyedJsonDocValues.create("berry", delegate);
        assertFalse(nonexistentDocValues.advanceExact(0));
    }

    public void testNextOrd() throws IOException {
        KeyedJsonDocValues docValues = KeyedJsonDocValues.create("banana", delegate);
        docValues.advanceExact(0);

        int retrievedOrds = 0;
        for (long ord = docValues.nextOrd(); ord != NO_MORE_ORDS; ord = docValues.nextOrd()) {
            assertTrue(30 <= ord && ord < 40);
            retrievedOrds++;

            BytesRef prefixedTerm = delegate.lookupOrd(ord);
            BytesRef key = JsonFieldParser.extractKey(prefixedTerm);
            assertEquals("banana", key.utf8ToString());
        }

        assertEquals(10, retrievedOrds);
    }

    public void testLookupOrd() throws IOException {
        KeyedJsonDocValues docValues = KeyedJsonDocValues.create("apple", delegate);

        BytesRef expectedValue = new BytesRef("value0");
        BytesRef value = docValues.lookupOrd(0);
        assertEquals(0, expectedValue.compareTo(value));
    }

    private static class MockSortedSetDocValues extends AbstractSortedSetDocValues {
        private final BytesRef[] allTerms;
        private final long[] documentOrds;
        private int index;

        MockSortedSetDocValues(BytesRef[] allTerms,
                               long[] documentOrds) {
            this.allTerms = allTerms;
            this.documentOrds = documentOrds;
        }

        @Override
        public boolean advanceExact(int docID) {
            index = 0;
            return true;
        }

        @Override
        public long nextOrd() {
            if (index == documentOrds.length) {
                return NO_MORE_ORDS;
            }
            return documentOrds[index++];
        }

        @Override
        public BytesRef lookupOrd(long ord) {
            return allTerms[(int) ord];
        }

        @Override
        public long getValueCount() {
            return allTerms.length;
        }
    }
}
