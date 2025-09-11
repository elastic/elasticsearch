/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.xpack.logsdb.patternedtext.PatternTextDocValuesTests.info;

public class PatternTextCompositeValuesTests extends ESTestCase {

    private static PatternedTextCompositeValues makeValues() throws IOException {
        // values:
        //  0: stored
        //  1: doc
        //  2: empty
        //  3: doc
        //  4: stored
        var templateId = new PatternTextDocValuesTests.SimpleSortedSetDocValues("id1", "id2", "id3", "id4", "id5");
        var template = new PatternTextDocValuesTests.SimpleSortedSetDocValues(null, " 2", null, " 4", null);
        var args = new PatternTextDocValuesTests.SimpleSortedSetDocValues(null, null, null, null, null);
        var info = new PatternTextDocValuesTests.SimpleSortedSetDocValues(PatternTextDocValuesTests.info(), info(0), info(), info(0), info());
        var patternedTextDocValues = new PatternedTextDocValues(template, args, info);

        String storedFieldName = "message.stored";
        var storedValues = List.of(new BytesRef(" 1"), null, null, null, new BytesRef(" 5"));
        var storedLoader = new SimpleStoredFieldLoader(storedValues, storedFieldName);
        return new PatternedTextCompositeValues(storedLoader, storedFieldName, patternedTextDocValues, templateId);
    }

    public void testKnownValues() throws IOException {
        var values = makeValues();

        assertEquals(-1, values.docID());
        assertEquals(0, value.nextDoc());
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


    static class SimpleStoredFieldLoader implements LeafStoredFieldLoader {
        private final List<BytesRef> values;
        private final String fieldName;
        private int doc = -1;

        SimpleStoredFieldLoader(List<BytesRef> values, String fieldName) {
            this.values = values;
            this.fieldName = fieldName;
        }

        @Override
        public void advanceTo(int doc) throws IOException {
            this.doc = doc;
        }

        @Override
        public BytesReference source() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String id() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String routing() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, List<Object>> storedFields() {
            return Map.of(fieldName, List.of(values.get(doc)));
        }
    }
}
