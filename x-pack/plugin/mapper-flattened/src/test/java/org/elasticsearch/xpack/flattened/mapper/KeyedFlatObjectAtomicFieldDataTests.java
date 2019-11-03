/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.flattened.mapper;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractAtomicOrdinalsFieldData;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

public class KeyedFlatObjectAtomicFieldDataTests extends ESTestCase {
    private AtomicOrdinalsFieldData delegate;

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

        delegate = new MockAtomicOrdinalsFieldData(allTerms, documentOrds);
    }

    private BytesRef prefixedValue(String key, String value) {
        String term = FlatObjectFieldParser.createKeyedValue(key, value);
        return new BytesRef(term);
    }

    public void testFindOrdinalBounds() throws IOException {
        testFindOrdinalBounds("apple", delegate, 0, 19);
        testFindOrdinalBounds("avocado", delegate, 20, 29);
        testFindOrdinalBounds("banana", delegate, 30, 39);
        testFindOrdinalBounds("berry", delegate, -1, -1);
        testFindOrdinalBounds("cantaloupe", delegate, 40, 40);
        testFindOrdinalBounds("cucumber", delegate, 41, 59);

        AtomicOrdinalsFieldData emptyDelegate = new MockAtomicOrdinalsFieldData(new BytesRef[0], new long[0]);
        testFindOrdinalBounds("apple", emptyDelegate, -1, -1);

        BytesRef[] terms = new BytesRef[] { prefixedValue("prefix", "value") };
        AtomicOrdinalsFieldData singleValueDelegate = new MockAtomicOrdinalsFieldData(terms, new long[0]);
        testFindOrdinalBounds("prefix", singleValueDelegate, 0, 0);
        testFindOrdinalBounds("prefix1", singleValueDelegate, -1, -1);

        terms = new BytesRef[] { prefixedValue("prefix", "value"),
            prefixedValue("prefix1", "value"),
            prefixedValue("prefix1", "value1"),
            prefixedValue("prefix2", "value"),
            prefixedValue("prefix3", "value")};
        AtomicOrdinalsFieldData oddLengthDelegate = new MockAtomicOrdinalsFieldData(terms, new long[0]);
        testFindOrdinalBounds("prefix", oddLengthDelegate, 0, 0);
        testFindOrdinalBounds("prefix1", oddLengthDelegate, 1, 2);
        testFindOrdinalBounds("prefix2", oddLengthDelegate, 3, 3);
        testFindOrdinalBounds("prefix3", oddLengthDelegate, 4, 4);
    }

    public void testFindOrdinalBounds(String key,
                                      AtomicOrdinalsFieldData delegate,
                                      long expectedMinOrd,
                                      long expectedMacOrd) throws IOException {
        BytesRef bytesKey = new BytesRef(key);

        long actualMinOrd = KeyedFlatObjectAtomicFieldData.findMinOrd(bytesKey, delegate.getOrdinalsValues());
        assertEquals(expectedMinOrd,  actualMinOrd);

        long actualMaxOrd = KeyedFlatObjectAtomicFieldData.findMaxOrd(bytesKey, delegate.getOrdinalsValues());
        assertEquals(expectedMacOrd, actualMaxOrd);
    }

    public void testAdvanceExact() throws IOException {
        AtomicOrdinalsFieldData avocadoFieldData = new KeyedFlatObjectAtomicFieldData("avocado", delegate);
        assertFalse(avocadoFieldData.getOrdinalsValues().advanceExact(0));

        AtomicOrdinalsFieldData bananaFieldData = new KeyedFlatObjectAtomicFieldData("banana", delegate);
        assertTrue(bananaFieldData.getOrdinalsValues().advanceExact(0));

        AtomicOrdinalsFieldData nonexistentFieldData = new KeyedFlatObjectAtomicFieldData("berry", delegate);
        assertFalse(nonexistentFieldData.getOrdinalsValues().advanceExact(0));
    }

    public void testNextOrd() throws IOException {
        AtomicOrdinalsFieldData fieldData = new KeyedFlatObjectAtomicFieldData("banana", delegate);
        SortedSetDocValues docValues = fieldData.getOrdinalsValues();
        docValues.advanceExact(0);

        int retrievedOrds = 0;
        for (long ord = docValues.nextOrd(); ord != NO_MORE_ORDS; ord = docValues.nextOrd()) {
            assertTrue(0 <= ord && ord < 10);
            retrievedOrds++;

            BytesRef expectedValue = new BytesRef("value" + (ord + 30));
            BytesRef actualValue = docValues.lookupOrd(ord);
            assertEquals(expectedValue, actualValue);
        }

        assertEquals(10, retrievedOrds);
    }

    public void testLookupOrd() throws IOException {
        AtomicOrdinalsFieldData appleFieldData = new KeyedFlatObjectAtomicFieldData("apple", delegate);
        SortedSetDocValues appleDocValues = appleFieldData.getOrdinalsValues();
        assertEquals(new BytesRef("value0"), appleDocValues.lookupOrd(0));

        AtomicOrdinalsFieldData cantaloupeFieldData = new KeyedFlatObjectAtomicFieldData("cantaloupe", delegate);
        SortedSetDocValues cantaloupeDocValues = cantaloupeFieldData.getOrdinalsValues();
        assertEquals(new BytesRef("value40"), cantaloupeDocValues.lookupOrd(0));

        AtomicOrdinalsFieldData cucumberFieldData = new KeyedFlatObjectAtomicFieldData("cucumber", delegate);
        SortedSetDocValues cucumberDocValues = cucumberFieldData.getOrdinalsValues();
        assertEquals(new BytesRef("value41"), cucumberDocValues.lookupOrd(0));
    }

    private static class MockAtomicOrdinalsFieldData extends AbstractAtomicOrdinalsFieldData {
        private final SortedSetDocValues docValues;

        MockAtomicOrdinalsFieldData(BytesRef[] allTerms,
                                    long[] documentOrds) {
            super(AbstractAtomicOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION);
            this.docValues = new MockSortedSetDocValues(allTerms, documentOrds);
        }

        @Override
        public SortedSetDocValues getOrdinalsValues() {
            return docValues;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public void close() {
            // Nothing to do.
        }
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
