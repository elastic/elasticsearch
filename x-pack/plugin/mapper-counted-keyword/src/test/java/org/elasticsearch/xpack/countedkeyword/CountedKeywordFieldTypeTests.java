/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.countedkeyword;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class CountedKeywordFieldTypeTests extends ESTestCase {
    public void testSingleValuedField() throws Exception {
        SortedSetDocValues sd = new CollectionBasedSortedSetDocValues(List.of(new BytesRef("a")));
        BinaryDocValues bd = new CollectionBasedBinaryDocValues(List.of(toBytesRef(new int[] { 3 })));

        CountedKeywordFieldMapper.CountedKeywordSortedBinaryDocValues dv =
            new CountedKeywordFieldMapper.CountedKeywordSortedBinaryDocValues(sd, bd);

        assertTrue(dv.advanceExact(0));

        assertEquals(3, dv.docValueCount());

        assertOrdinal(dv, "a", 3);
    }

    public void testMultiValuedField() throws Exception {
        SortedSetDocValues sd = new CollectionBasedSortedSetDocValues(List.of(new BytesRef("a"), new BytesRef("b")));
        BinaryDocValues bd = new CollectionBasedBinaryDocValues(List.of(toBytesRef(new int[] { 1, 5 })));

        CountedKeywordFieldMapper.CountedKeywordSortedBinaryDocValues dv =
            new CountedKeywordFieldMapper.CountedKeywordSortedBinaryDocValues(sd, bd);

        assertTrue(dv.advanceExact(0));

        assertEquals(6, dv.docValueCount());

        assertOrdinal(dv, "a", 1);
        assertOrdinal(dv, "b", 5);
    }

    private void assertOrdinal(CountedKeywordFieldMapper.CountedKeywordSortedBinaryDocValues dv, String value, int times)
        throws IOException {
        for (int i = 0; i < times; i++) {
            long ordinal = dv.nextOrd();
            assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, ordinal);
            assertEquals(new BytesRef(value), dv.lookupOrd(ordinal));
        }
    }

    private BytesRef toBytesRef(int[] counts) throws IOException {
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            streamOutput.writeVIntArray(counts);
            return streamOutput.bytes().toBytesRef();
        }
    }

    private static class CollectionBasedSortedSetDocValues extends SortedSetDocValues {
        private final List<BytesRef> docValues;

        private final DocIdSetIterator disi;

        private long currentOrd = -1;

        private CollectionBasedSortedSetDocValues(List<BytesRef> docValues) {
            this.docValues = docValues;
            this.disi = DocIdSetIterator.all(docValues.size());
        }

        @Override
        public long nextOrd() {
            return ++currentOrd;
        }

        @Override
        public int docValueCount() {
            return docValues.size();
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            return docValues.get((int) ord);
        }

        @Override
        public long getValueCount() {
            return docValues.size();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            currentOrd = -1;
            return disi.advance(target) == target;
        }

        @Override
        public int docID() {
            return disi.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            currentOrd = -1;
            return disi.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            currentOrd = -1;
            return disi.advance(target);
        }

        @Override
        public long cost() {
            return disi.cost();
        }
    }

    private static class CollectionBasedBinaryDocValues extends BinaryDocValues {
        private final List<BytesRef> docValues;
        private final DocIdSetIterator disi;

        private int current = -1;

        private CollectionBasedBinaryDocValues(List<BytesRef> docValues) {
            this.docValues = docValues;
            this.disi = DocIdSetIterator.all(docValues.size());
        }

        @Override
        public BytesRef binaryValue() {
            return docValues.get(current);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            current = target;
            return disi.advance(target) == target;
        }

        @Override
        public int docID() {
            return disi.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            current = -1;
            return disi.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            current = -1;
            return disi.advance(target);
        }

        @Override
        public long cost() {
            return disi.cost();
        }
    }

}
