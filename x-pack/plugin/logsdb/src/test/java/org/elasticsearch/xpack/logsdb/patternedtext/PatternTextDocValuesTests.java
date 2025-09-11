/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.apache.lucene.index.BinaryDocValues;
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

    enum Storage {
        DOC_VALUE,
        STORED_FIELD,
        EMPTY
    }

    record Message(Storage storage, boolean hasArg, String message) {
        // Arg is just the first character
        String arg() {
            if (storage == Storage.DOC_VALUE) {
                return hasArg ? message.substring(0, 1) : null;
            }
            return null;
        }

        String template() {
            if (storage == Storage.DOC_VALUE) {
                return hasArg ? message.substring(1) : message;
            }
            return null;
        }
    }

    private static List<Message> makeRandomMessages(int numDocs, boolean includeStored) {
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            // if arg is present, it's at the beginning
            Storage storage = includeStored ? randomFrom(Storage.values()) : randomFrom(Storage.DOC_VALUE, Storage.EMPTY);
            String message = randomAlphaOfLength(10) + " " + i;
            boolean hasArg = storage == Storage.DOC_VALUE && randomBoolean();
            messages.add(new Message(storage, hasArg, storage == Storage.EMPTY ? null : message));
        }
        return messages;
    }

    private static BinaryDocValues makeDocValues(List<Message> messages) throws IOException {
        var template = new SimpleSortedSetDocValues(messages.stream().map(Message::template).toList().toArray(new String[0]));
        var args = new SimpleSortedSetDocValues(messages.stream().map(Message::arg).toList().toArray(new String[0]));
        var info = new SimpleSortedSetDocValues(messages.stream().map(m -> m.hasArg() ? info(0) : info()).toList().toArray(new String[0]));
        return new PatternedTextDocValues(template, args, info);
    }

    private static BinaryDocValues makeDocValueSparseArgs() throws IOException {
        return makeDocValues(
            List.of(
                new Message(Storage.DOC_VALUE, true, "1 dog"),
                new Message(Storage.DOC_VALUE, false, "mouse"),
                new Message(Storage.DOC_VALUE, true, "3 cat"),
                new Message(Storage.DOC_VALUE, false,"house")
            )
        );
    }

    private static BinaryDocValues makeDocValueMissingValues() throws IOException {
        return makeDocValues(
            List.of(
                new Message(Storage.DOC_VALUE, true, "1 dog"),
                new Message(Storage.EMPTY, false, null),
                new Message(Storage.DOC_VALUE, true, "3 cat"),
                new Message(Storage.EMPTY, false, null)
            )
        );
    }

    public void testValueAll() throws IOException {
        var docValues = makeDocValueSparseArgs();
        assertEquals(0, docValues.nextDoc());
        assertEquals("1 dog", docValues.binaryValue().utf8ToString());
        assertEquals(1, docValues.nextDoc());
        assertEquals("mouse", docValues.binaryValue().utf8ToString());
        assertEquals(2, docValues.nextDoc());
        assertEquals("3 cat", docValues.binaryValue().utf8ToString());
        assertEquals(3, docValues.nextDoc());
        assertEquals("house", docValues.binaryValue().utf8ToString());
    }

    public void testValueMissing() throws IOException {
        var docValues = makeDocValueMissingValues();
        assertEquals(0, docValues.nextDoc());
        assertEquals("1 dog", docValues.binaryValue().utf8ToString());
        assertEquals(2, docValues.nextDoc());
        assertEquals("3 cat", docValues.binaryValue().utf8ToString());
    }

    public void testRandomMessages() throws IOException {
        List<Message> messages = makeRandomMessages(randomIntBetween(0, 100), false);
        BinaryDocValues docValues = makeDocValues(messages);
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            if (message.storage == Storage.EMPTY) {
                assertFalse(docValues.advanceExact(i));
            } else {
                assertTrue(docValues.advanceExact(i));
                assertEquals(message.message, docValues.binaryValue().utf8ToString());
            }
        }
    }

    public void testNextDoc() throws IOException {
        var docValues = makeDocValueSparseArgs();
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
        assertEquals(2, docValues.nextDoc());
        assertEquals(NO_MORE_DOCS, docValues.nextDoc());
    }

    public void testAdvance1() throws IOException {
        var docValues = makeDocValueSparseArgs();
        assertEquals(-1, docValues.docID());
        assertEquals(0, docValues.nextDoc());
        assertEquals(1, docValues.advance(1));
        assertEquals(2, docValues.advance(2));
        assertEquals(3, docValues.advance(3));
        assertEquals(NO_MORE_DOCS, docValues.advance(4));
    }

    public void testAdvanceFarther() throws IOException {
        var docValues = makeDocValueSparseArgs();
        assertEquals(2, docValues.advance(2));
        // repeats so stay on value
        assertEquals(2, docValues.advance(2));
    }

    public void testAdvanceSkipsValuesIfMissing() throws IOException {
        var docValues = makeDocValueMissingValues();
        assertEquals(2, docValues.advance(1));
    }

    public void testAdvanceExactMissing() throws IOException {
        var docValues = makeDocValueMissingValues();
        assertTrue(docValues.advanceExact(0));
        assertFalse(docValues.advanceExact(1));
        assertEquals(2, docValues.docID());
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

    public static String info(int... offsets) {
        List<Arg.Info> argsInfo = new ArrayList<>();
        for (var offset : offsets) {
            argsInfo.add(new Arg.Info(Arg.Type.GENERIC, offset));
        }
        try {
            return Arg.encodeInfo(argsInfo);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
