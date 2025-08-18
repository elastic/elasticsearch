/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class PatternTextDocValuesTests extends ESTestCase {

    private static final String TIMESTAMP = "2020-09-06T08:29:04.123Z";
    private static final long MILLIS = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(TIMESTAMP);

    private static PatternedTextDocValues makeDocValueSparseArgs() {
        var template = new SimpleSortedSetDocValues("%W dog %T", "cat", "%W mouse %T %W", "hat %W");
        var args = new SimpleSortedSetDocValues("1", null, "2 3", "4");
        var ts = new SimpleSortedNumericDocValues(MILLIS, null, MILLIS, null);
        return new PatternedTextDocValues(template, args, ts);
    }

    private static PatternedTextDocValues makeDocValuesDenseArgs() {
        var template = new SimpleSortedSetDocValues("%W moose %T", "%T %W goose %W", "%W mouse %T %W", "%W house %T");
        var args = new SimpleSortedSetDocValues("1", "4 5", "2 3", "7");
        var ts = new SimpleSortedNumericDocValues(MILLIS, MILLIS, MILLIS, MILLIS);
        return new PatternedTextDocValues(template, args, ts);
    }

    private static PatternedTextDocValues makeDocValueMissingValues() {
        var template = new SimpleSortedSetDocValues("%W cheddar %T", "cat", null, "%W cheese");
        var args = new SimpleSortedSetDocValues("1", null, null, "4");
        var ts = new SimpleSortedNumericDocValues(MILLIS, null, null, null);
        return new PatternedTextDocValues(template, args, ts);
    }

    public void testNextDoc() throws IOException {
        var docValues = randomBoolean() ? makeDocValueSparseArgs() : makeDocValuesDenseArgs();
        assertEquals(-1, docValues.docID());
        assertEquals(0, docValues.nextDoc());
        assertEquals(1, docValues.nextDoc());
        assertEquals(2, docValues.nextDoc());
        assertEquals(3, docValues.nextDoc());
        assertEquals(NO_MORE_DOCS, docValues.nextDoc());
    }

    public void testNextDocMissing() throws IOException {
        var docValues = makeDocValueMissingValues();
        assertEquals(-1, docValues.docID());
        assertEquals(0, docValues.nextDoc());
        assertEquals(1, docValues.nextDoc());
        assertEquals(3, docValues.nextDoc());
        assertEquals(NO_MORE_DOCS, docValues.nextDoc());
    }

    public void testAdvance1() throws IOException {
        var docValues = randomBoolean() ? makeDocValueSparseArgs() : makeDocValuesDenseArgs();
        assertEquals(-1, docValues.docID());
        assertEquals(0, docValues.nextDoc());
        assertEquals(1, docValues.advance(1));
        assertEquals(2, docValues.advance(2));
        assertEquals(3, docValues.advance(3));
        assertEquals(NO_MORE_DOCS, docValues.advance(4));
    }

    public void testAdvanceFarther() throws IOException {
        var docValues = randomBoolean() ? makeDocValueSparseArgs() : makeDocValuesDenseArgs();
        assertEquals(2, docValues.advance(2));
        // repeats says on value
        assertEquals(2, docValues.advance(2));
    }

    public void testAdvanceSkipsValuesIfMissing() throws IOException {
        var docValues = makeDocValueMissingValues();
        assertEquals(3, docValues.advance(2));
    }

    public void testAdvanceExactMissing() throws IOException {
        var docValues = makeDocValueMissingValues();
        assertTrue(docValues.advanceExact(1));
        assertFalse(docValues.advanceExact(2));
        assertEquals(3, docValues.docID());
    }

    public void testValueAll() throws IOException {
        var docValues = makeDocValuesDenseArgs();
        assertEquals(0, docValues.nextDoc());
        assertEquals(String.format("1 moose %s", TIMESTAMP), docValues.binaryValue().utf8ToString());
        assertEquals(1, docValues.nextDoc());
        assertEquals(String.format("%s 4 goose 5", TIMESTAMP), docValues.binaryValue().utf8ToString());
        assertEquals(2, docValues.nextDoc());
        assertEquals(String.format("2 mouse %s 3", TIMESTAMP), docValues.binaryValue().utf8ToString());
        assertEquals(3, docValues.nextDoc());
        assertEquals(String.format("7 house %s", TIMESTAMP), docValues.binaryValue().utf8ToString());
    }

    public void testValueMissing() throws IOException {
        var docValues = makeDocValueMissingValues();
        assertEquals(0, docValues.nextDoc());
        assertEquals("1 cheddar " + TIMESTAMP, docValues.binaryValue().utf8ToString());
        assertEquals(1, docValues.nextDoc());
        assertEquals("cat", docValues.binaryValue().utf8ToString());
        assertEquals(3, docValues.nextDoc());
        assertEquals("4 cheese", docValues.binaryValue().utf8ToString());
    }

    static class SimpleSortedNumericDocValues extends SortedNumericDocValues {

        private final Long[] docIdToValues;
        private int currDoc = -1;

        // Single value for each docId, null if no value for a docId
        SimpleSortedNumericDocValues(Long... docIdToValues) {
            this.docIdToValues = docIdToValues;
        }

        @Override
        public long nextValue() throws IOException {
            assert docIdToValues[currDoc] != null;
            return docIdToValues[currDoc];
        }

        @Override
        public int docValueCount() {
            return 1;
        }

        @Override
        public boolean advanceExact(int target) {
            return advance(target) == target;
        }

        @Override
        public int docID() {
            return currDoc >= docIdToValues.length ? NO_MORE_DOCS : currDoc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(currDoc + 1);
        }

        @Override
        public int advance(int target) {
            for (currDoc = target; currDoc < docIdToValues.length; currDoc++) {
                if (docIdToValues[currDoc] != null) {
                    return currDoc;
                }
            }
            return NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return 1;
        }
    }

    static class SimpleSortedSetDocValues extends SortedSetDocValues {

        private final List<String> ordToValues;
        private final List<Integer> docToOrds;
        private int currDoc = -1;

        // Single value for each docId, null if no value for a docId
        SimpleSortedSetDocValues(String... docIdToValue) {
            ordToValues = Arrays.stream(docIdToValue).filter(Objects::nonNull).collect(Collectors.toSet()).stream().sorted().toList();
            docToOrds = Arrays.stream(docIdToValue).map(v -> v == null ? null : ordToValues.indexOf(v)).toList();
        }

        @Override
        public long nextOrd() {
            return docToOrds.get(currDoc);
        }

        @Override
        public int docValueCount() {
            return 1;
        }

        @Override
        public BytesRef lookupOrd(long ord) {
            return new BytesRef(ordToValues.get((int) ord));
        }

        @Override
        public long getValueCount() {
            return ordToValues.size();
        }

        @Override
        public boolean advanceExact(int target) {
            return advance(target) == target;
        }

        @Override
        public int docID() {
            return currDoc >= docToOrds.size() ? NO_MORE_DOCS : currDoc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(currDoc + 1);
        }

        @Override
        public int advance(int target) {
            for (currDoc = target; currDoc < docToOrds.size(); currDoc++) {
                if (docToOrds.get(currDoc) != null) {
                    return currDoc;
                }
            }
            return NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return 1;
        }
    }
}
