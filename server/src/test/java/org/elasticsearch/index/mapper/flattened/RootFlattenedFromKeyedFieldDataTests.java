/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RootFlattenedFromKeyedFieldDataTests extends ESTestCase {

    public void testSingleKeyedValue() throws IOException {
        var wrapper = createWrapper("key1\0value1");
        assertTrue(wrapper.advanceExact(0));
        assertEquals(1, wrapper.docValueCount());
        assertEquals(new BytesRef("value1"), wrapper.nextValue());
    }

    public void testMultipleKeysUniqueValues() throws IOException {
        var wrapper = createWrapper("key1\0alpha", "key2\0beta", "key3\0gamma");
        assertTrue(wrapper.advanceExact(0));
        assertEquals(3, wrapper.docValueCount());
        assertEquals(new BytesRef("alpha"), wrapper.nextValue());
        assertEquals(new BytesRef("beta"), wrapper.nextValue());
        assertEquals(new BytesRef("gamma"), wrapper.nextValue());
    }

    public void testDuplicateValuesAcrossKeys() throws IOException {
        var wrapper = createWrapper("key1\0foo", "key2\0foo", "key3\0bar");
        assertTrue(wrapper.advanceExact(0));
        assertEquals(2, wrapper.docValueCount());
        assertEquals(new BytesRef("bar"), wrapper.nextValue());
        assertEquals(new BytesRef("foo"), wrapper.nextValue());
    }

    public void testAllDuplicateValues() throws IOException {
        var wrapper = createWrapper("a\0same", "b\0same", "c\0same");
        assertTrue(wrapper.advanceExact(0));
        assertEquals(1, wrapper.docValueCount());
        assertEquals(new BytesRef("same"), wrapper.nextValue());
    }

    public void testValuesAreSorted() throws IOException {
        var wrapper = createWrapper("key1\0zebra", "key2\0apple", "key3\0mango");
        assertTrue(wrapper.advanceExact(0));
        assertEquals(3, wrapper.docValueCount());
        assertEquals(new BytesRef("apple"), wrapper.nextValue());
        assertEquals(new BytesRef("mango"), wrapper.nextValue());
        assertEquals(new BytesRef("zebra"), wrapper.nextValue());
    }

    public void testEmptyDocument() throws IOException {
        var delegate = new FakeSortedBinaryDocValues(List.of());
        var wrapper = new RootFlattenedFromKeyedFieldData.KeyStrippingDeduplicatingDocValues(delegate);
        assertFalse(wrapper.advanceExact(0));
    }

    private RootFlattenedFromKeyedFieldData.KeyStrippingDeduplicatingDocValues createWrapper(String... keyedValues) {
        List<BytesRef> refs = new ArrayList<>();
        for (String kv : keyedValues) {
            refs.add(new BytesRef(kv));
        }
        var delegate = new FakeSortedBinaryDocValues(refs);
        return new RootFlattenedFromKeyedFieldData.KeyStrippingDeduplicatingDocValues(delegate);
    }

    private static class FakeSortedBinaryDocValues extends SortedBinaryDocValues {
        private final List<BytesRef> values;
        private int index;

        FakeSortedBinaryDocValues(List<BytesRef> values) {
            this.values = values;
        }

        @Override
        public boolean advanceExact(int target) {
            index = 0;
            return values.isEmpty() == false;
        }

        @Override
        public int docValueCount() {
            return values.size();
        }

        @Override
        public BytesRef nextValue() {
            return values.get(index++);
        }
    }
}
