/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.zstd;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.bwc.Elasticsearch93Lucene104Codec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reads Zstd814 stored fields through long-lived {@link StoredFields} instances, the way search fetches hits, so that the
 * decompressor is asked for several documents of the same block in a row and keeps blocks decompressed. Covers chunks that
 * are split into slices because they end with a large document, visitors that stop early or skip fields, documents without
 * stored fields, repeated and backward reads, and two instances used in turn.
 */
public class Zstd814StoredFieldsReuseTests extends ESTestCase {

    private static final String ID_FIELD = "id";
    private static final String BODY_FIELD = "body";
    private static final String NUMBER_FIELD = "n";

    public void testBestSpeed() throws IOException {
        doTestReuse(Zstd814StoredFieldsFormat.Mode.BEST_SPEED);
    }

    public void testBestCompression() throws IOException {
        doTestReuse(Zstd814StoredFieldsFormat.Mode.BEST_COMPRESSION);
    }

    /** The stored fields of a document, {@code id == null} for a document without stored fields. */
    private record Doc(String id, int number, byte[] body) {}

    /** Which fields a visitor loads; fields are stored in the order id, body, number. */
    private enum VisitorMode {
        ALL,
        ID_THEN_STOP,
        NUMBER_ONLY,
        BODY_THEN_STOP
    }

    private static void doTestReuse(Zstd814StoredFieldsFormat.Mode mode) throws IOException {
        final int chunkSize = mode.blockSizeInBytes;
        final int numDocs = randomIntBetween(300, 1500);
        final List<Doc> docs = new ArrayList<>(numDocs);
        final List<Integer> largeDocs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            final int roll = randomIntBetween(0, 99);
            if (roll < 2 && i != numDocs / 2) {
                docs.add(new Doc(null, 0, null));
                continue;
            }
            final int bodyLength;
            if (roll < 4 || i == numDocs / 2) {
                // at least twice the chunk size, so the chunk holding this document and the ones before it is sliced
                bodyLength = 2 * chunkSize + randomIntBetween(0, chunkSize);
                largeDocs.add(i);
            } else if (roll < 6) {
                bodyLength = chunkSize + randomIntBetween(0, chunkSize - 1);
            } else if (roll < 10) {
                bodyLength = 0;
            } else {
                bodyLength = randomIntBetween(1, 3000);
            }
            final byte[] body = bodyLength == 0 ? null : randomAlphaOfLength(bodyLength).getBytes(StandardCharsets.UTF_8);
            docs.add(new Doc("doc-" + i, randomInt(), body));
        }

        try (Directory dir = randomBoolean() ? new MMapDirectory(createTempDir()) : new NIOFSDirectory(createTempDir())) {
            final IndexWriterConfig config = new IndexWriterConfig().setCodec(new Elasticsearch93Lucene104Codec(mode));
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (Doc doc : docs) {
                    final Document document = new Document();
                    if (doc.id() != null) {
                        document.add(new StoredField(ID_FIELD, doc.id()));
                        if (doc.body() != null) {
                            document.add(new StoredField(BODY_FIELD, new BytesRef(doc.body())));
                        }
                        document.add(new StoredField(NUMBER_FIELD, doc.number()));
                    }
                    writer.addDocument(document);
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertEquals(numDocs, leaf.maxDoc());

                // all documents in order through one instance
                final StoredFields sequential = leaf.storedFields();
                for (int docId = 0; docId < numDocs; docId++) {
                    assertDocument(sequential, docId, docs.get(docId));
                }

                // a random walk with repeats, short steps forward and backward and jumps, on two instances used in turn
                final StoredFields first = leaf.storedFields();
                final StoredFields second = leaf.storedFields();
                final int steps = randomIntBetween(500, 2000);
                int docId = 0;
                for (int i = 0; i < steps; i++) {
                    docId = switch (randomIntBetween(0, 3)) {
                        case 0 -> docId;
                        case 1 -> Math.min(numDocs - 1, docId + randomIntBetween(1, 5));
                        case 2 -> Math.max(0, docId - randomIntBetween(1, 5));
                        case 3 -> randomIntBetween(0, numDocs - 1);
                        default -> throw new AssertionError();
                    };
                    assertDocument(randomIntBetween(0, 3) == 0 ? second : first, docId, docs.get(docId));
                }

                // the documents that share a chunk with each large document, in order, shuffled and reversed, with repeats
                final StoredFields aroundLargeDocs = leaf.storedFields();
                for (int largeDoc : largeDocs) {
                    final List<Integer> docIds = new ArrayList<>();
                    for (int i = Math.max(0, largeDoc - 16); i <= Math.min(numDocs - 1, largeDoc + 2); i++) {
                        docIds.add(i);
                        if (randomBoolean()) {
                            docIds.add(i);
                        }
                    }
                    for (int pass = 0; pass < 3; pass++) {
                        if (pass == 1) {
                            Collections.shuffle(docIds, random());
                        } else if (pass == 2) {
                            Collections.reverse(docIds);
                        }
                        for (int id : docIds) {
                            assertDocument(aroundLargeDocs, id, docs.get(id));
                        }
                    }
                }
            }
        }
    }

    private static void assertDocument(StoredFields storedFields, int docId, Doc expected) throws IOException {
        final VisitorMode mode = randomFrom(VisitorMode.values());
        final RecordingVisitor visitor = new RecordingVisitor(mode);
        storedFields.document(docId, visitor);
        final String message = "doc [" + docId + "] visitor [" + mode + "]";
        if (expected.id() == null) {
            assertNull(message, visitor.id);
            assertNull(message, visitor.number);
            assertNull(message, visitor.body);
            return;
        }
        switch (mode) {
            case ALL -> {
                assertEquals(message, expected.id(), visitor.id);
                assertEquals(message, Integer.valueOf(expected.number()), visitor.number);
                assertArrayEquals(message, expected.body(), visitor.body);
            }
            case ID_THEN_STOP -> {
                assertEquals(message, expected.id(), visitor.id);
                assertNull(message, visitor.number);
                assertNull(message, visitor.body);
            }
            case NUMBER_ONLY -> {
                assertNull(message, visitor.id);
                assertEquals(message, Integer.valueOf(expected.number()), visitor.number);
                assertNull(message, visitor.body);
            }
            case BODY_THEN_STOP -> {
                assertNull(message, visitor.id);
                assertNull(message, visitor.number);
                assertArrayEquals(message, expected.body(), visitor.body);
            }
        }
    }

    private static class RecordingVisitor extends StoredFieldVisitor {
        private final VisitorMode mode;
        String id;
        Integer number;
        byte[] body;

        RecordingVisitor(VisitorMode mode) {
            this.mode = mode;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            return switch (mode) {
                case ALL -> Status.YES;
                case ID_THEN_STOP -> fieldInfo.name.equals(ID_FIELD) ? Status.YES : Status.STOP;
                case NUMBER_ONLY -> fieldInfo.name.equals(NUMBER_FIELD) ? Status.YES : Status.NO;
                case BODY_THEN_STOP -> fieldInfo.name.equals(BODY_FIELD) ? Status.YES : (body == null ? Status.NO : Status.STOP);
            };
        }

        @Override
        public void stringField(FieldInfo fieldInfo, String value) {
            id = value;
        }

        @Override
        public void intField(FieldInfo fieldInfo, int value) {
            number = value;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            body = value;
        }
    }
}
