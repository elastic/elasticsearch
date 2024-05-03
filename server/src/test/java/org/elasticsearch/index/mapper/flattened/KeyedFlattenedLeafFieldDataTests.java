/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

public class KeyedFlattenedLeafFieldDataTests extends ESTestCase {
    private LeafOrdinalsFieldData delegate;

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

        delegate = new MockLeafOrdinalsFieldData(allTerms, documentOrds);
    }

    private BytesRef prefixedValue(String key, String value) {
        String term = FlattenedFieldParser.createKeyedValue(key, value);
        return new BytesRef(term);
    }

    public void testFindOrdinalBounds() throws IOException {
        testFindOrdinalBounds("apple", delegate, 0, 19);
        testFindOrdinalBounds("avocado", delegate, 20, 29);
        testFindOrdinalBounds("banana", delegate, 30, 39);
        testFindOrdinalBounds("berry", delegate, -1, -1);
        testFindOrdinalBounds("cantaloupe", delegate, 40, 40);
        testFindOrdinalBounds("cucumber", delegate, 41, 59);

        LeafOrdinalsFieldData emptyDelegate = new MockLeafOrdinalsFieldData(new BytesRef[0], new long[0]);
        testFindOrdinalBounds("apple", emptyDelegate, -1, -1);

        BytesRef[] terms = new BytesRef[] { prefixedValue("prefix", "value") };
        LeafOrdinalsFieldData singleValueDelegate = new MockLeafOrdinalsFieldData(terms, new long[0]);
        testFindOrdinalBounds("prefix", singleValueDelegate, 0, 0);
        testFindOrdinalBounds("prefix1", singleValueDelegate, -1, -1);

        terms = new BytesRef[] {
            prefixedValue("prefix", "value"),
            prefixedValue("prefix1", "value"),
            prefixedValue("prefix1", "value1"),
            prefixedValue("prefix2", "value"),
            prefixedValue("prefix3", "value") };
        LeafOrdinalsFieldData oddLengthDelegate = new MockLeafOrdinalsFieldData(terms, new long[0]);
        testFindOrdinalBounds("prefix", oddLengthDelegate, 0, 0);
        testFindOrdinalBounds("prefix1", oddLengthDelegate, 1, 2);
        testFindOrdinalBounds("prefix2", oddLengthDelegate, 3, 3);
        testFindOrdinalBounds("prefix3", oddLengthDelegate, 4, 4);
    }

    public void testFindOrdinalBounds(String key, LeafOrdinalsFieldData delegate, long expectedMinOrd, long expectedMacOrd)
        throws IOException {
        BytesRef bytesKey = new BytesRef(key);

        long actualMinOrd = KeyedFlattenedLeafFieldData.findMinOrd(bytesKey, delegate.getOrdinalsValues());
        assertEquals(expectedMinOrd, actualMinOrd);

        long actualMaxOrd = KeyedFlattenedLeafFieldData.findMaxOrd(bytesKey, delegate.getOrdinalsValues());
        assertEquals(expectedMacOrd, actualMaxOrd);
    }

    public void testAdvanceExact() throws IOException {
        LeafOrdinalsFieldData avocadoFieldData = new KeyedFlattenedLeafFieldData("avocado", delegate, MOCK_TO_SCRIPT_FIELD);
        assertFalse(avocadoFieldData.getOrdinalsValues().advanceExact(0));

        LeafOrdinalsFieldData bananaFieldData = new KeyedFlattenedLeafFieldData("banana", delegate, MOCK_TO_SCRIPT_FIELD);
        assertTrue(bananaFieldData.getOrdinalsValues().advanceExact(0));

        LeafOrdinalsFieldData nonexistentFieldData = new KeyedFlattenedLeafFieldData("berry", delegate, MOCK_TO_SCRIPT_FIELD);
        assertFalse(nonexistentFieldData.getOrdinalsValues().advanceExact(0));
    }

    public void testNextOrd() throws IOException {
        LeafOrdinalsFieldData fieldData = new KeyedFlattenedLeafFieldData("banana", delegate, MOCK_TO_SCRIPT_FIELD);
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
        LeafOrdinalsFieldData appleFieldData = new KeyedFlattenedLeafFieldData("apple", delegate, MOCK_TO_SCRIPT_FIELD);
        SortedSetDocValues appleDocValues = appleFieldData.getOrdinalsValues();
        assertEquals(new BytesRef("value0"), appleDocValues.lookupOrd(0));

        LeafOrdinalsFieldData cantaloupeFieldData = new KeyedFlattenedLeafFieldData("cantaloupe", delegate, MOCK_TO_SCRIPT_FIELD);
        SortedSetDocValues cantaloupeDocValues = cantaloupeFieldData.getOrdinalsValues();
        assertEquals(new BytesRef("value40"), cantaloupeDocValues.lookupOrd(0));

        LeafOrdinalsFieldData cucumberFieldData = new KeyedFlattenedLeafFieldData("cucumber", delegate, MOCK_TO_SCRIPT_FIELD);
        SortedSetDocValues cucumberDocValues = cucumberFieldData.getOrdinalsValues();
        assertEquals(new BytesRef("value41"), cucumberDocValues.lookupOrd(0));
    }

    private static class MockLeafOrdinalsFieldData extends AbstractLeafOrdinalsFieldData {
        private final SortedSetDocValues docValues;

        MockLeafOrdinalsFieldData(BytesRef[] allTerms, long[] documentOrds) {
            super(MOCK_TO_SCRIPT_FIELD);
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

    private static final ToScriptFieldFactory<SortedSetDocValues> MOCK_TO_SCRIPT_FIELD = (dv, n) -> new DelegateDocValuesField(
        new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(FieldData.toString(dv))),
        n
    );

    private static class MockSortedSetDocValues extends AbstractSortedSetDocValues {
        private final BytesRef[] allTerms;
        private final long[] documentOrds;
        private int index;

        MockSortedSetDocValues(BytesRef[] allTerms, long[] documentOrds) {
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
        public int docValueCount() {
            return documentOrds.length;
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
