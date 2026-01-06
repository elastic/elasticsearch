/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

        static Message stored(String message) {
            return new Message(Storage.STORED_FIELD, false, message);
        }

        static Message withArg(String message) {
            return new Message(Storage.DOC_VALUE, true, message);
        }

        static Message noArg(String message) {
            return new Message(Storage.DOC_VALUE, false, message);
        }

        static Message empty() {
            return new Message(Storage.EMPTY, false, null);
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

    private static BinaryDocValues makeDocValues(List<Message> messages) {
        var template = new SimpleSortedSetDocValues(messages.stream().map(Message::template).toList().toArray(new String[0]));
        var args = new SimpleBinaryDocValues(messages.stream().map(Message::arg).toList().toArray(new String[0]));
        var info = new SimpleSortedSetDocValues(messages.stream().map(m -> m.hasArg() ? info(0) : info()).toList().toArray(new String[0]));
        return new PatternTextDocValues(template, args, info);
    }

    private static BinaryDocValues makeCompositeDocValues(List<Message> messages) throws IOException {
        var patternTextDocValues = makeDocValues(messages);
        var templateId = new SimpleSortedSetDocValues(
            IntStream.range(0, messages.size())
                .mapToObj(i -> messages.get(i).storage == Storage.EMPTY ? null : "id" + i)
                .toList()
                .toArray(new String[0])
        );
        String storedFieldName = "message.stored";
        var storedValues = messages.stream().map(m -> m.storage == Storage.STORED_FIELD ? new BytesRef(m.message) : null).toList();
        var storedLoader = new SimpleStoredFieldLoader(storedValues, storedFieldName);
        return new PatternTextCompositeValues(storedLoader, storedFieldName, patternTextDocValues, templateId);
    }

    private static BinaryDocValues makeDocValuesDense() throws IOException {
        return makeDocValues(List.of(Message.withArg("1 a"), Message.noArg("2 b"), Message.withArg("3 c"), Message.noArg("4 d")));
    }

    private static BinaryDocValues makeDocValueMissingValues() throws IOException {
        return makeDocValues(
            List.of(Message.noArg("1 a"), Message.empty(), Message.withArg("3 c"), Message.empty(), Message.noArg("5 e"), Message.empty())
        );
    }

    private static BinaryDocValues makeCompositeDense() throws IOException {
        return makeCompositeDocValues(List.of(Message.stored("1 a"), Message.withArg("2 b"), Message.stored("3 c"), Message.noArg("4 d")));
    }

    private static BinaryDocValues makeCompositeMissingValues() throws IOException {
        return makeCompositeDocValues(
            List.of(Message.stored("1 a"), Message.empty(), Message.withArg("3 c"), Message.empty(), Message.noArg("5 e"), Message.empty())
        );
    }

    private static BinaryDocValues denseValues() throws IOException {
        return randomBoolean() ? makeDocValuesDense() : makeCompositeDense();
    }

    private static BinaryDocValues sparseValues() throws IOException {
        return randomBoolean() ? makeDocValueMissingValues() : makeCompositeMissingValues();
    }

    public void testDenseValues() throws IOException {
        var docValues = denseValues();
        assertTrue(docValues.advanceExact(0));
        assertEquals("1 a", docValues.binaryValue().utf8ToString());
        assertTrue(docValues.advanceExact(1));
        assertEquals("2 b", docValues.binaryValue().utf8ToString());
        assertTrue(docValues.advanceExact(2));
        assertEquals("3 c", docValues.binaryValue().utf8ToString());
        assertTrue(docValues.advanceExact(3));
        assertEquals("4 d", docValues.binaryValue().utf8ToString());
    }

    public void testSparseValues() throws IOException {
        var docValues = sparseValues();
        assertTrue(docValues.advanceExact(0));
        assertEquals("1 a", docValues.binaryValue().utf8ToString());
        assertFalse(docValues.advanceExact(1));
        assertTrue(docValues.advanceExact(2));
        assertEquals("3 c", docValues.binaryValue().utf8ToString());
        assertFalse(docValues.advanceExact(3));
        assertTrue(docValues.advanceExact(4));
        assertEquals("5 e", docValues.binaryValue().utf8ToString());
    }

    public void testRandomMessagesDocValues() throws IOException {
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

    public void testRandomMessagesComposite() throws IOException {
        List<Message> messages = makeRandomMessages(randomIntBetween(0, 100), true);
        BinaryDocValues docValues = makeCompositeDocValues(messages);
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

    public void testAdvanceSkipsValuesIfMissing() throws IOException {
        var docValues = sparseValues();
        assertFalse(docValues.advanceExact(1));
        assertEquals(2, docValues.docID());
    }

    public void testAdvanceExactMissing() throws IOException {
        var docValues = sparseValues();
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

        public boolean advanceExact(int target) {
            currDoc = target;

            // If there is a value for target, currDoc is set to target, and we return true because target was found.
            if (docToOrds.get(currDoc) != null) {
                return true;
            }

            // Otherwise, we update currDoc to first doc with a value after target and return false.
            while (currDoc < docToOrds.size() && docToOrds.get(currDoc) == null) {
                currDoc++;
            }
            return false;
        }

        @Override
        public int docID() {
            return currDoc >= docToOrds.size() ? NO_MORE_DOCS : currDoc;
        }

        @Override
        public int nextDoc() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            return 1;
        }
    }

    static class SimpleBinaryDocValues extends BinaryDocValues {

        private final List<String> values;
        private int currDoc = -1;

        // Single value for each docId, null if no value for a docId
        SimpleBinaryDocValues(String... docIdToValue) {
            values = Arrays.asList(docIdToValue);
        }

        @Override
        public BytesRef binaryValue() {
            return new BytesRef(values.get(currDoc));
        }

        @Override
        public boolean advanceExact(int target) {
            currDoc = target;

            // If there is a value for target, currDoc is set to target, and we return true because target was found.
            if (values.get(currDoc) != null) {
                return true;
            }

            // Otherwise, we update currDoc to first doc with a value after target and return false.
            while (currDoc < values.size() && values.get(currDoc) == null) {
                currDoc++;
            }
            return false;
        }

        @Override
        public int docID() {
            return currDoc >= values.size() ? NO_MORE_DOCS : currDoc;
        }

        @Override
        public int nextDoc() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
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
