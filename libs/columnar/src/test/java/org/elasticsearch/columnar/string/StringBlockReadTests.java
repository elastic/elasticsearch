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
import java.util.List;

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

    /** A column with no dictionary always hands over values. */
    public void testPlainColumnComesBackAsValues() throws IOException {
        final String[] terms = { "red", "green", "blue" };
        final BytesRef[] docValues = new BytesRef[between(500, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        assertPages(docValues, DictionaryPolicy.NONE, Shape.VALUES);
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
            final int[] docs = new int[docValues.length];
            for (int d = 0; d < docs.length; d++) {
                docs[d] = d;
            }
            final int dictionarySize = reader.dictionarySize();
            final java.util.Map<String, Integer> byTerm = new java.util.HashMap<>();
            final BytesRef scratch = new BytesRef();
            final int page = 128;
            final int[] ordinals = new int[page];
            for (int from = 0; from < docs.length; from += page) {
                final int count = Math.min(page, docs.length - from);
                assertTrue("expected ordinals", reader.readOrdinals(docs, from, count, ordinals));
                for (int i = 0; i < count; i++) {
                    final String value = docValues[from + i].utf8ToString();
                    final int ordinal = ordinals[i];
                    if (ordinal >= dictionarySize) {
                        assertEquals("escaped [" + value + "]", value, reader.resolveEscape(docs[from + i], scratch).utf8ToString());
                    } else {
                        assertEquals("ordinal [" + ordinal + "]", value, reader.termAt(ordinal, new BytesRef()).utf8ToString());
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
