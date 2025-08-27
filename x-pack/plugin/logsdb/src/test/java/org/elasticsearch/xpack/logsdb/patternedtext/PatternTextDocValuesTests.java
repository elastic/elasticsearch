/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class PatternTextDocValuesTests extends ESTestCase {

    private static PatternedTextDocValues makeDocValueSparseArgs() throws IOException {
        var template = new SimpleSortedSetDocValues(removePlaceholders("% dog", "cat", "% mouse %", "hat %"));
        var args = new SimpleSortedSetDocValues("1", null, "2 3", "4");
        var info = new SimpleSortedSetDocValues(info(0), info(), info(0, 7), info(4));
        return new PatternedTextDocValues(template, args, info);
    }

    private static PatternedTextDocValues makeDocValuesDenseArgs() throws IOException {
        var template = new SimpleSortedSetDocValues(removePlaceholders("% moose", "% goose %", "% mouse %", "% house"));
        var args = new SimpleSortedSetDocValues("1", "4 5", "2 3", "7");
        var info = new SimpleSortedSetDocValues(info(0), info(0, 7), info(0, 7), info(0));
        return new PatternedTextDocValues(template, args, info);
    }

    private static PatternedTextDocValues makeDocValueMissingValues() throws IOException {
        var template = new SimpleSortedSetDocValues(removePlaceholders("% cheddar", "cat", null, "% cheese"));
        var args = new SimpleSortedSetDocValues("1", null, null, "4");
        var info = new SimpleSortedSetDocValues(info(0), info(), info(), info(0));
        return new PatternedTextDocValues(template, args, info);
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
        assertEquals("1 moose", docValues.binaryValue().utf8ToString());
        assertEquals(1, docValues.nextDoc());
        assertEquals("4 goose 5", docValues.binaryValue().utf8ToString());
        assertEquals(2, docValues.nextDoc());
        assertEquals("2 mouse 3", docValues.binaryValue().utf8ToString());
        assertEquals(3, docValues.nextDoc());
        assertEquals("7 house", docValues.binaryValue().utf8ToString());
    }

    public void testValueMissing() throws IOException {
        var docValues = makeDocValueMissingValues();
        assertEquals(0, docValues.nextDoc());
        assertEquals("1 cheddar", docValues.binaryValue().utf8ToString());
        assertEquals(1, docValues.nextDoc());
        assertEquals("cat", docValues.binaryValue().utf8ToString());
        assertEquals(3, docValues.nextDoc());
        assertEquals("4 cheese", docValues.binaryValue().utf8ToString());
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

    private static String info(int... offsets) throws IOException {
        List<Arg.Info> argsInfo = new ArrayList<>();
        for (var offset : offsets) {
            argsInfo.add(new Arg.Info(Arg.Type.GENERIC, offset));
        }
        return Arg.encodeInfo(argsInfo);
    }

    // Placeholders are only included here to help in testing
    private static String[] removePlaceholders(String... values) {
        return Arrays.stream(values).map(s -> s == null ? null : s.replace("%", "")).toList().toArray(String[]::new);
    }
}
