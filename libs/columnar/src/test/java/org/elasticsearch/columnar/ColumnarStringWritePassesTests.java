/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.columnar.string.SummaryPolicy;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;

/**
 * How many times writing a string column goes back to Lucene's in-memory field, and how many values it
 * pulls out of it. On the ingest path every pass over the values copies each payload out of the indexing
 * buffer and splits it again, so a pass saved is a copy per document saved, and a pass that only needs the
 * documents must not touch the payloads at all.
 *
 * <p>Counted at the {@link DocValuesProducer} the consumer is handed: {@code getBinary} is one cursor, one
 * pass, and {@code binaryValue} is one payload copied. What the counts should be follows from the writer:
 * <ul>
 * <li>a column is counted and surveyed in one pass, which reads every payload once;</li>
 * <li>a sparse column then writes its presence from a cursor of its own, which moves through the documents
 * and reads no payload;</li>
 * <li>the values are then written in one more pass, dictionary or plain, which reads every payload once.</li>
 * </ul>
 * A dictionary column streams its ordinals as it writes them and stages only its escapes, which it replays
 * from a temporary file rather than from the field, so it takes no pass more than a plain column does. So a
 * column takes two cursors and two reads a document when dense, and three cursors and still two reads a
 * document when sparse, whether it is written as a dictionary or plain and whether or not the survey found
 * anything.
 */
public class ColumnarStringWritePassesTests extends ESTestCase {

    private static final String FIELD = "field";

    /** The longest value a dictionary's term table admits; anything longer is never a candidate. */
    private static final int MAX_TERM_BYTES = ByteBlockPool.BYTE_BLOCK_SIZE - 2;

    /**
     * A few repeated terms over every document: the count-and-survey pass, then the dictionary pass, which
     * streams the ordinals and needs no replay of them from the field.
     */
    public void testDenseDictionaryColumn() throws IOException {
        final BytesRef[][] docSlots = repeating(between(100, 400), false);
        final Passes passes = write(docSlots, StringColumnOptions.DEFAULT, true);
        assertEquals("cursors", 2, passes.cursors());
        assertEquals("payloads read", 2L * docsWithField(docSlots), passes.values());
    }

    /**
     * The same terms over half the documents: the presence pass is one cursor more, but it reads no payload,
     * so the payloads are read as often as in the dense shape.
     */
    public void testSparseDictionaryColumn() throws IOException {
        final BytesRef[][] docSlots = repeating(between(200, 600), true);
        final Passes passes = write(docSlots, StringColumnOptions.DEFAULT, true);
        assertEquals("cursors", 3, passes.cursors());
        assertEquals("payloads read", 2L * docsWithField(docSlots), passes.values());
    }

    /** No dictionary allowed: a count-only pass, then the plain pass, each reading every payload once. */
    public void testDensePlainColumnWithoutADictionaryPolicy() throws IOException {
        final BytesRef[][] docSlots = repeating(between(100, 400), false);
        final StringColumnOptions options = StringColumnOptions.DEFAULT.withPolicies(DictionaryPolicy.NONE, SummaryPolicy.NONE);
        final Passes passes = write(docSlots, options, false);
        assertEquals("cursors", 2, passes.cursors());
        assertEquals("payloads read", 2L * docsWithField(docSlots), passes.values());
    }

    /** As above over half the documents: the presence pass adds a cursor and reads nothing. */
    public void testSparsePlainColumnWithoutADictionaryPolicy() throws IOException {
        final BytesRef[][] docSlots = repeating(between(200, 600), true);
        final StringColumnOptions options = StringColumnOptions.DEFAULT.withPolicies(DictionaryPolicy.NONE, SummaryPolicy.NONE);
        final Passes passes = write(docSlots, options, false);
        assertEquals("cursors", 3, passes.cursors());
        assertEquals("payloads read", 2L * docsWithField(docSlots), passes.values());
    }

    /**
     * A dictionary allowed but every value distinct: the survey finds terms and the policy turns the
     * dictionary down, and the column is written plain without being surveyed a second time.
     */
    public void testDensePlainColumnWhoseDictionaryIsNotWorthKeeping() throws IOException {
        final BytesRef[][] docSlots = distinct(between(100, 400), false);
        final Passes passes = write(docSlots, StringColumnOptions.DEFAULT, false);
        assertEquals("cursors", 2, passes.cursors());
        assertEquals("payloads read", 2L * docsWithField(docSlots), passes.values());
    }

    /**
     * Every value longer than a term table holds, so the survey finds nothing at all. A survey that found
     * nothing is still a survey, and the writer must not run another one on the strength of its empty answer.
     */
    public void testDenseColumnWhereTheSurveyFindsNothingWorthNaming() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(10, 30)][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            final byte[] bytes = new byte[MAX_TERM_BYTES + between(1, 16)];
            Arrays.fill(bytes, (byte) ('a' + doc % 26));
            docSlots[doc] = new BytesRef[] { new BytesRef(bytes) };
        }
        final Passes passes = write(docSlots, StringColumnOptions.DEFAULT, false);
        assertEquals("cursors", 2, passes.cursors());
        assertEquals("payloads read", 2L * docSlots.length, passes.values());
    }

    /** The sparse shape of the same: a presence pass, and still no second survey. */
    public void testSparseColumnWhereTheSurveyFindsNothingWorthNaming() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(20, 60)][];
        for (int doc = 0; doc < docSlots.length; doc += 2) {
            final byte[] bytes = new byte[MAX_TERM_BYTES + between(1, 16)];
            Arrays.fill(bytes, (byte) ('a' + doc % 26));
            docSlots[doc] = new BytesRef[] { new BytesRef(bytes) };
        }
        final Passes passes = write(docSlots, StringColumnOptions.DEFAULT, false);
        assertEquals("cursors", 3, passes.cursors());
        assertEquals("payloads read", 2L * docsWithField(docSlots), passes.values());
    }

    /** What the consumer asked of the field: cursors opened, and payloads read through them. */
    private record Passes(int cursors, long values) {}

    /**
     * Writes {@code docSlots} as one segment through a format that counts what the consumer asks of the
     * field, reads the column back to check it survived the passes it took, and reports the counts.
     */
    private Passes write(BytesRef[][] docSlots, StringColumnOptions options, boolean expectDictionary) throws IOException {
        final int[] cursors = new int[1];
        final long[] values = new long[1];
        final ColumNARDocValuesFormat columnar = new ColumNARDocValuesFormat(
            (fieldName, type) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            StringColumnOptionsSelector.always(options)
        );
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(counting(columnar, cursors, values)));
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (BytesRef[] slots : docSlots) {
                    final Document doc = new Document();
                    if (slots != null) {
                        doc.add(new Field(FIELD, payload(slots), columnarBinaryFieldType()));
                    }
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("one segment", 1, reader.leaves().size());
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues dv = leaf.getBinaryDocValues(FIELD);
                assertTrue("a columnar column", dv instanceof ColumnarStringBinaryDocValues);
                assertEquals("layout", expectDictionary, ((ColumnarStringBinaryDocValues) dv).reader().hasDictionary());
                for (int doc = 0; doc < docSlots.length; doc++) {
                    if (docSlots[doc] == null) {
                        continue;
                    }
                    assertEquals("doc " + doc, doc, dv.nextDoc());
                    assertEquals("doc " + doc, payload(docSlots[doc]), dv.binaryValue());
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
            }
        }
        return new Passes(cursors[0], values[0]);
    }

    /**
     * {@code columnar} behind a format that counts, for every binary field it is handed, the cursors the
     * consumer opens over the field and the payloads it reads through them. Only the write side is
     * wrapped: reading goes to the columnar format as it is.
     */
    private static DocValuesFormat counting(ColumNARDocValuesFormat columnar, int[] cursors, long[] values) {
        return new DocValuesFormat(ColumnarFormat.NAME) {
            @Override
            public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
                final DocValuesConsumer consumer = columnar.fieldsConsumer(state);
                return new DocValuesConsumer() {
                    @Override
                    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                        consumer.addBinaryField(field, new EmptyDocValuesProducer() {
                            @Override
                            public BinaryDocValues getBinary(FieldInfo info) throws IOException {
                                cursors[0]++;
                                return counting(valuesProducer.getBinary(info), values);
                            }
                        });
                    }

                    @Override
                    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                        consumer.addNumericField(field, valuesProducer);
                    }

                    @Override
                    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                        consumer.addSortedField(field, valuesProducer);
                    }

                    @Override
                    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                        consumer.addSortedNumericField(field, valuesProducer);
                    }

                    @Override
                    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                        consumer.addSortedSetField(field, valuesProducer);
                    }

                    @Override
                    public void merge(MergeState mergeState) throws IOException {
                        consumer.merge(mergeState);
                    }

                    @Override
                    public void close() throws IOException {
                        consumer.close();
                    }
                };
            }

            @Override
            public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
                return columnar.fieldsProducer(state);
            }
        };
    }

    /** {@code in} with every {@code binaryValue} call counted; the positions pass straight through. */
    private static BinaryDocValues counting(BinaryDocValues in, long[] values) {
        return new BinaryDocValues() {
            @Override
            public BytesRef binaryValue() throws IOException {
                values[0]++;
                return in.binaryValue();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return in.advanceExact(target);
            }

            @Override
            public int docID() {
                return in.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return in.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return in.advance(target);
            }

            @Override
            public long cost() {
                return in.cost();
            }
        };
    }

    /** One of a few terms a document, over every document or over every other one. */
    private static BytesRef[][] repeating(int maxDoc, boolean sparse) {
        final String[] terms = { "INFO", "WARN", "ERROR", "DEBUG" };
        final BytesRef[][] docSlots = new BytesRef[maxDoc][];
        for (int doc = 0; doc < maxDoc; doc++) {
            if (sparse && doc % 2 == 1) {
                continue;
            }
            docSlots[doc] = new BytesRef[] { new BytesRef(terms[doc % terms.length]) };
        }
        return docSlots;
    }

    /** A value of its own for every document, so no dictionary is worth keeping. */
    private static BytesRef[][] distinct(int maxDoc, boolean sparse) {
        final BytesRef[][] docSlots = new BytesRef[maxDoc][];
        for (int doc = 0; doc < maxDoc; doc++) {
            if (sparse && doc % 2 == 1) {
                continue;
            }
            docSlots[doc] = new BytesRef[] { new BytesRef("distinct-" + doc + "-" + randomAlphaOfLength(12)) };
        }
        return docSlots;
    }

    private static int docsWithField(BytesRef[][] docSlots) {
        int count = 0;
        for (BytesRef[] slots : docSlots) {
            if (slots != null) {
                count++;
            }
        }
        return count;
    }

    /** The payload the mapper writes for {@code slots}, owning its bytes so it outlives the builder. */
    private static BytesRef payload(BytesRef[] slots) {
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(Arrays.asList(slots)));
    }
}
