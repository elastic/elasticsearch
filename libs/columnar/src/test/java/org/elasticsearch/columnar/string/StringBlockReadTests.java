/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/**
 * Reading a page of values for a consumer that groups over them. Whichever shape the page comes back in,
 * it has to rebuild exactly the values the documents hold, so every case is checked against them.
 */
public class StringBlockReadTests extends ColumnarStringTestCase {

    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

    /** Few terms over many documents, which is where ordinals are worth handing over. */
    public void testRepetitivePageComesBackAsOrdinals() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final BytesRef[] docValues = new BytesRef[between(500, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        assertPages(docValues, ROOMY, Shape.ORDINALS);
    }

    /** Every value distinct, so a page of ordinals is as long as the page and the values come back instead. */
    public void testDistinctPageComesBackAsValues() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(500, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef("id-" + d);
        }
        assertPages(docValues, ROOMY, Shape.VALUES);
    }

    /** A column with no dictionary, every value distinct, so the values come back as themselves. */
    public void testPlainColumnOfDistinctValuesComesBackAsValues() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(500, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef("plain-" + d);
        }
        assertPages(docValues, DictionaryPolicy.NONE, Shape.VALUES);
    }

    /**
     * A column with no dictionary whose values rotate, so no two documents that are next to each other hold
     * the same one. A page still holds three values however long it is, and it is those the ordinals name.
     */
    public void testPlainColumnOfRotatingValuesComesBackAsOrdinals() throws IOException {
        final String[] terms = { "red", "green", "blue" };
        final BytesRef[] docValues = new BytesRef[between(500, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        assertPages(docValues, DictionaryPolicy.NONE, Shape.ORDINALS);
    }

    /**
     * An ordinal names a value, so a page hands back one slot a value however many documents carry it and
     * wherever they sit. A value returning to a page after another one is the case coalescing what arrives
     * together cannot see: stored twice it is two addresses, which say nothing about the bytes being equal.
     */
    public void testAValueReturningToAPageKeepsItsOrdinal() throws IOException {
        for (DictionaryPolicy policy : List.of(DictionaryPolicy.NONE, ROOMY)) {
            final BytesRef[] docValues = new BytesRef[600];
            for (int d = 0; d < docValues.length; d++) {
                // Runs that restart, the shape a column clustered by region rather than sorted has.
                docValues[d] = new BytesRef((d / 2) % 2 == 0 ? "podA" : "podB");
            }
            assertDistinctOrdinals("rotating runs", docValues, policy);
        }
    }

    /**
     * Runs longer than a {@link ValueStream#VALUES_PER_BLOCK} block. A run is staged a block at a time, so a
     * run reaching into the next block is stored again and read at an address the one before it did not have.
     * Coalescing what arrives together cannot tell that from a new value, so this is the shape that takes a
     * slot a block rather than a slot a value, and it is the shape a column in term order has most of.
     */
    public void testRunsLongerThanABlockKeepOneSlotAValue() throws IOException {
        final String[] terms = { "aaa", "bbb", "ccc" };
        final BytesRef[] docValues = new BytesRef[2400];
        for (int d = 0; d < docValues.length; d++) {
            // Runs of 300 over blocks of 128, so every run crosses at least two block boundaries.
            docValues[d] = new BytesRef(terms[(d / 300) % terms.length]);
        }
        assertDistinctOrdinals("runs longer than a block", docValues, DictionaryPolicy.NONE, 128, new int[] { 128, 512, 1024 });
    }

    /**
     * The same, for values the dictionary does not name. Two documents holding the same escaped bytes are
     * one value, and the page has no ordinal to tell it by, only the bytes.
     */
    public void testARepeatedEscapedValueKeepsItsOrdinal() throws IOException {
        final BytesRef[] docValues = new BytesRef[3000];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = d % 7 == 3 ? new BytesRef("tail-" + d) : new BytesRef("common-" + (d % 3));
        }
        // Held by two documents of one page, and still dropped from a vocabulary this small.
        docValues[5] = new BytesRef("tail-shared");
        docValues[100] = new BytesRef("tail-shared");
        assertDistinctOrdinals("repeated escape", docValues, new DictionaryPolicy(64, 0.1, 0.9));
    }

    /** Every value of a page appears in its dictionary once, whatever the column's shape. */
    private void assertDistinctOrdinals(String label, BytesRef[] docValues, DictionaryPolicy policy) throws IOException {
        assertDistinctOrdinals(label, docValues, policy, 512, new int[] { Math.min(512, docValues.length) });
    }

    /** The same, over the whole column a page at a time, under a block size the caller chooses. */
    private void assertDistinctOrdinals(String label, BytesRef[] docValues, DictionaryPolicy policy, int blockSize, int[] pageSizes)
        throws IOException {
        withColumn(docValues, blockSize, randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
            for (int pageSize : pageSizes) {
                for (int from = 0; from < docValues.length; from += pageSize) {
                    assertPageOfOrdinals(label + " at " + from + " in pages of " + pageSize, docValues, reader, from, pageSize);
                }
            }
        });
    }

    private void assertPageOfOrdinals(String label, BytesRef[] docValues, StringColumnReader reader, int from, int pageSize)
        throws IOException {
        final int count = Math.min(pageSize, docValues.length - from);
        final int[] docs = new int[count];
        for (int i = 0; i < count; i++) {
            docs[i] = from + i;
        }
        {
            final boolean[] sawOrdinals = { false };
            assertTrue(label + " page", reader.readBlock(docs, 0, docs.length, new StringBlockSink() {
                @Override
                public void appendOrdinals(int[] ordinals, int count, BytesRef[] dictionary, int dictionarySize) {
                    sawOrdinals[0] = true;
                    final Map<String, Integer> slotOf = new HashMap<>();
                    for (int i = 0; i < dictionarySize; i++) {
                        final String value = dictionary[i].utf8ToString();
                        final Integer first = slotOf.putIfAbsent(value, i);
                        assertNull(label + " holds [" + value + "] at slots " + first + " and " + i, first);
                    }
                    // And the ordinals still name the values the documents hold.
                    for (int i = 0; i < count; i++) {
                        assertEquals(label + " doc " + docs[i], docValues[docs[i]].utf8ToString(), dictionary[ordinals[i]].utf8ToString());
                    }
                }

                @Override
                public void appendValues(BytesRef[] values, int count) {
                    for (int i = 0; i < count; i++) {
                        assertEquals(label + " doc " + docs[i], docValues[docs[i]].utf8ToString(), values[i].utf8ToString());
                    }
                }
            }));
            assertTrue(label + " expected a page of ordinals", sawOrdinals[0]);
        }
    }

    /** Escaped documents are their own entries, and have to rebuild as themselves. */
    public void testPageWithEscapes() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final BytesRef[] docValues = new BytesRef[between(800, 2500)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = d % 50 == 3 ? new BytesRef("escaped-" + d) : new BytesRef(terms[d % terms.length]);
        }
        assertPages(docValues, ROOMY, Shape.ANY);
    }

    /** A dictionary larger than the page, so the page's ordinals are sorted rather than indexed. */
    public void testDictionaryLargerThanThePage() throws IOException {
        final BytesRef[] docValues = new BytesRef[4000];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef("t" + (d % 900));
        }
        assertPages(docValues, ROOMY, Shape.ANY);
    }

    /**
     * The ordinals a consumer counts into a dense array with. They name the column's own terms, so the same
     * term has to come back as the same ordinal wherever it is read, and an escaped value has to be marked
     * and resolvable to its bytes.
     */
    public void testOrdinalsNameTheColumnsTerms() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final BytesRef[] docValues = new BytesRef[between(800, 2500)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = d % 50 == 3 ? new BytesRef("escaped-" + d) : new BytesRef(terms[d % terms.length]);
        }
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, (metadata, reader) -> {
            assertTrue("expected a dictionary", reader.hasDictionary());
            final DictionaryStringColumnReader dictionary = (DictionaryStringColumnReader) reader;
            final int[] docs = new int[docValues.length];
            for (int d = 0; d < docs.length; d++) {
                docs[d] = d;
            }
            final int dictionarySize = reader.dictionarySize();
            final java.util.Map<String, Integer> byTerm = new java.util.HashMap<>();
            final BytesRef scratch = new BytesRef();
            final int page = 128;
            final int[] ordinals = new int[page];
            final int[] ranks = new int[page];
            for (int from = 0; from < docs.length; from += page) {
                final int count = Math.min(page, docs.length - from);
                assertTrue("expected ordinals", reader.readOrdinals(docs, from, count, ordinals));
                // An escaped value is reached by its address, which is what a document's rank gives; the two
                // only coincide on a column every document has a value in.
                reader.ranks(docs, from, count, ranks);
                for (int i = 0; i < count; i++) {
                    final String value = docValues[from + i].utf8ToString();
                    final int ordinal = ordinals[i];
                    if (ordinal >= dictionarySize) {
                        final long address = reader.firstValueAddress(ranks[i]);
                        assertEquals("escaped [" + value + "]", value, dictionary.resolveEscape(address, scratch).utf8ToString());
                    } else {
                        assertEquals("ordinal [" + ordinal + "]", value, dictionary.termAt(ordinal, new BytesRef()).utf8ToString());
                        final Integer seen = byTerm.putIfAbsent(value, ordinal);
                        assertTrue("term [" + value + "] took two ordinals", seen == null || seen == ordinal);
                    }
                }
            }
            assertEquals("every repeated term named", terms.length, byTerm.size());
        });
    }

    /** A column storing its values has no ordinals to serve, and says so rather than inventing them. */
    public void testPlainColumnServesNoOrdinals() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(200, 800)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef("v" + (d % 5));
        }
        withColumn(
            docValues,
            randomValidBlockSize(),
            randomChunkCodec(),
            randomTargetChunkBytes(),
            DictionaryPolicy.NONE,
            (metadata, reader) -> {
                assertFalse("expected no dictionary", reader.hasDictionary());
                final int[] docs = { 0, 1, 2, 3 };
                assertFalse("a plain column has no column-wide ordinals", reader.readOrdinals(docs, 0, docs.length, new int[docs.length]));
            }
        );
    }

    /**
     * A column not every document has a value in, asked about documents that do not. A page has no way to
     * say a document has no value, so the read has to decline rather than answer with someone else's.
     */
    public void testSparsePageIsDeclined() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final BytesRef[] docValues = new BytesRef[between(500, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = d % 3 == 1 ? null : new BytesRef(terms[d % terms.length]);
        }
        for (DictionaryPolicy policy : List.of(ROOMY, DictionaryPolicy.NONE)) {
            withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                final int[] all = new int[docValues.length];
                for (int d = 0; d < all.length; d++) {
                    all[d] = d;
                }
                final int[] ordinals = new int[all.length];
                assertFalse(
                    "a page covering documents with no value cannot be served",
                    reader.readBlock(all, 0, all.length, new StringBlockSink() {
                        @Override
                        public void appendOrdinals(int[] ords, int n, BytesRef[] dictionary, int dictionarySize) {}

                        @Override
                        public void appendValues(BytesRef[] values, int n) {}
                    })
                );
                if (reader.hasDictionary()) {
                    assertFalse("ordinals cannot be served for documents with no value", reader.readOrdinals(all, 0, all.length, ordinals));
                }
                // The documents that do have a value are still served.
                final List<Integer> present = new ArrayList<>();
                for (int d = 0; d < docValues.length; d++) {
                    if (docValues[d] != null) {
                        present.add(d);
                    }
                }
                final int[] dense = present.stream().mapToInt(Integer::intValue).toArray();
                final List<String> seen = new ArrayList<>();
                assertTrue(
                    "a page of documents that all have a value is served",
                    reader.readBlock(dense, 0, dense.length, new StringBlockSink() {
                        @Override
                        public void appendOrdinals(int[] ords, int n, BytesRef[] dictionary, int dictionarySize) {
                            for (int i = 0; i < n; i++) {
                                seen.add(dictionary[ords[i]].utf8ToString());
                            }
                        }

                        @Override
                        public void appendValues(BytesRef[] values, int n) {
                            for (int i = 0; i < n; i++) {
                                seen.add(values[i].utf8ToString());
                            }
                        }
                    })
                );
                final List<String> want = new ArrayList<>();
                for (int doc : dense) {
                    want.add(docValues[doc].utf8ToString());
                }
                assertEquals("values of the documents that have one", want, seen);
            });
        }
    }

    /** Documents read out of order, and not all of them, which is what a filtered aggregation hands over. */
    public void testScatteredPage() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie", "delta" };
        final BytesRef[] docValues = new BytesRef[2000];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        final List<Integer> picked = new ArrayList<>();
        for (int d = 0; d < docValues.length; d += 7) {
            picked.add(d);
        }
        java.util.Collections.shuffle(picked, random());
        // Pages are handed over in document order, so a scattered page is still ascending within itself.
        final int[] docs = picked.subList(0, 256).stream().sorted().mapToInt(Integer::intValue).toArray();
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, (metadata, reader) -> {
            assertPage(reader, docValues, docs, 0, docs.length, Shape.ANY);
        });
    }

    private enum Shape {
        ORDINALS,
        VALUES,
        ANY
    }

    private void assertPages(BytesRef[] docValues, DictionaryPolicy policy, Shape shape) throws IOException {
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
            final int[] docs = new int[docValues.length];
            for (int d = 0; d < docs.length; d++) {
                docs[d] = d;
            }
            // Several page sizes, since the page's own storage and the dictionary index both key off it.
            for (int page : new int[] { 1, 16, 128, 512 }) {
                for (int from = 0; from < docs.length; from += page) {
                    final int count = Math.min(page, docs.length - from);
                    // A page too short to repeat is no shorter as ordinals, and correctly comes back as
                    // values whatever the column looks like, so only the shape of a real page is asserted.
                    assertPage(reader, docValues, docs, from, count, count >= 16 ? shape : Shape.ANY);
                }
            }
        });
    }

    private void assertPage(StringColumnReader reader, BytesRef[] docValues, int[] docs, int offset, int count, Shape shape)
        throws IOException {
        final Rebuilt rebuilt = new Rebuilt();
        assertTrue("page served", reader.readBlock(docs, offset, count, rebuilt));
        if (shape == Shape.ORDINALS) {
            assertTrue("expected ordinals at page " + offset, rebuilt.wasOrdinals);
        }
        if (shape == Shape.VALUES) {
            assertFalse("expected values at page " + offset, rebuilt.wasOrdinals);
        }
        assertEquals("page size at " + offset, count, rebuilt.values.size());
        for (int i = 0; i < count; i++) {
            assertEquals("doc " + docs[offset + i], docValues[docs[offset + i]], rebuilt.values.get(i));
        }
    }

    /** Rebuilds a page into plain values, whichever shape it arrived in. */
    private static final class Rebuilt implements StringBlockSink {

        private final List<BytesRef> values = new ArrayList<>();
        private boolean wasOrdinals;

        @Override
        public void appendOrdinals(int[] ordinals, int count, BytesRef[] dictionary, int dictionarySize) {
            wasOrdinals = true;
            for (int i = 0; i < count; i++) {
                assertTrue("ordinal in range", ordinals[i] >= 0 && ordinals[i] < dictionarySize);
                values.add(BytesRef.deepCopyOf(dictionary[ordinals[i]]));
            }
        }

        @Override
        public void appendValues(BytesRef[] pageValues, int count) {
            for (int i = 0; i < count; i++) {
                values.add(BytesRef.deepCopyOf(pageValues[i]));
            }
        }
    }
}
