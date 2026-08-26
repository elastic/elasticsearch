/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * A column stored as an ordinal per value into a dictionary of its terms.
 *
 * <p>Which layout a column takes is a property of its values, so these tests state the values and assert
 * the layout, rather than asking for one. Only a vocabulary that names every value is written today, so a
 * column holding anything the dictionary would not is expected to stay plain.
 */
public class StringDictionaryTests extends ColumnarStringTestCase {

    /**
     * An escaped value resolves to the same bytes however it is asked for. Counting an escape's place
     * carries on from the value answered before it, so a caller that goes back or jumps has to fall back
     * to counting from the start of the block rather than from wherever the last caller left off.
     */
    public void testEscapesResolveInAnyOrder() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final BytesRef[] docValues = new BytesRef[2000];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = d % 7 == 3 ? new BytesRef("escaped-" + d) : new BytesRef(terms[d % terms.length]);
        }
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, (metadata, reader) -> {
            assertTrue("expected a dictionary", reader.hasDictionary());
            final List<Integer> escaped = new ArrayList<>();
            for (int d = 0; d < docValues.length; d++) {
                if (d % 7 == 3) {
                    escaped.add(d);
                }
            }
            for (int doc : escaped) {
                assertEquals("ascending [" + doc + "]", docValues[doc], reader.valueAt(reader.firstValueAddress(doc)));
            }
            for (int i = escaped.size() - 1; i >= 0; i--) {
                final int doc = escaped.get(i);
                assertEquals("descending [" + doc + "]", docValues[doc], reader.valueAt(reader.firstValueAddress(doc)));
                assertEquals("repeated [" + doc + "]", docValues[doc], reader.valueAt(reader.firstValueAddress(doc)));
            }
            final List<Integer> shuffled = new ArrayList<>(escaped);
            java.util.Collections.shuffle(shuffled, random());
            for (int doc : shuffled) {
                assertEquals("shuffled [" + doc + "]", docValues[doc], reader.valueAt(reader.firstValueAddress(doc)));
            }
        });
    }

    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

    /** A handful of terms over many documents: every value is named, and reads back as itself. */
    public void testRepeatedTermsTakeTheDictionary() throws IOException {
        final String[] terms = { "DEBUG", "ERROR", "INFO", "TRACE", "WARN" };
        final BytesRef[] docValues = new BytesRef[between(500, 3000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertEquals("one ordinal per distinct term", terms.length, dictionaryOf(metadata).dictionarySize());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /** Nothing repeats, so there is no vocabulary and the values are stored as they are. */
    public void testAllDistinctValuesStayPlain() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(200, 1500)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef("id-" + d);
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.PLAIN, metadata.layout());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /** A term seen once is turned away by the survey, so its value escapes rather than taking an ordinal. */
    public void testOneUnrepeatedValueEscapes() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final BytesRef[] docValues = new BytesRef[between(300, 900)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        docValues[between(0, docValues.length - 1)] = new BytesRef("seen-exactly-once");
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertTrue("the lone value escaped", dictionaryOf(metadata).hasEscapes());
            assertEquals("one escape", 1L, dictionaryOf(metadata).escapes().numValues());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /**
     * A head of repeated terms over a long tail seen once each. The head takes ordinals and the tail
     * escapes, which is the shape most real columns have.
     */
    public void testHeadTakesOrdinalsAndTailEscapes() throws IOException {
        final String[] head = { "alpha", "bravo", "charlie", "delta" };
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 4000; i++) {
            values.add(new BytesRef(head[i % head.length]));
        }
        for (int i = 0; i < 400; i++) {
            values.add(new BytesRef("rare-" + i));
        }
        java.util.Collections.shuffle(values, random());
        final BytesRef[] docValues = values.toArray(BytesRef[]::new);
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertEquals("the head is the dictionary", head.length, dictionaryOf(metadata).dictionarySize());
            assertEquals("the tail escaped", 400L, dictionaryOf(metadata).escapes().numValues());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /**
     * Escapes far apart, so a value is reached from a rank-table entry several blocks back and the count
     * of escapes between has to be right.
     */
    public void testEscapesSpreadAcrossManyBlocks() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(2000, 5000)];
        for (int d = 0; d < docValues.length; d++) {
            // Roughly one in three hundred is unique, so most blocks hold no escape at all.
            docValues[d] = d % 300 == 7 ? new BytesRef("unique-" + d) : new BytesRef(d % 2 == 0 ? "on" : "off");
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertTrue("some values escaped", dictionaryOf(metadata).hasEscapes());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /** Every value escaping is the degenerate case: the rank of one is its own position. */
    public void testEscapesInEveryPosition() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(300, 800)];
        for (int d = 0; d < docValues.length; d++) {
            // Two terms carry the dictionary; everything else is distinct and escapes.
            docValues[d] = d % 50 == 0 ? new BytesRef(d % 100 == 0 ? "yes" : "no") : new BytesRef("x-" + d);
        }
        withDictionary(docValues, (metadata, reader) -> assertEveryValueReadsBack(docValues, reader));
    }

    /** A column that escapes nothing writes no escape stream and no rank table. */
    public void testNoEscapesWritesNoExceptions() throws IOException {
        final String[] terms = { "DEBUG", "ERROR", "INFO" };
        final BytesRef[] docValues = new BytesRef[between(300, 900)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertFalse("nothing escaped", dictionaryOf(metadata).hasEscapes());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /** A dictionary as large as the values it stands in for has bought nothing, so it is not written. */
    public void testDictionaryTooLargeAgainstTheColumnIsRefused() throws IOException {
        // Every term appears twice, so the vocabulary is complete, but it is half the column's bytes.
        final BytesRef[] docValues = new BytesRef[600];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef("value-" + (d / 2));
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.PLAIN, metadata.layout());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /** Documents without a value are skipped by the iterator, so their absence does not shift an ordinal. */
    public void testGapsAmongDictionaryValues() throws IOException {
        final String[] terms = { "red", "green", "blue" };
        final BytesRef[] docValues = new BytesRef[between(400, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = randomBoolean() ? null : new BytesRef(terms[d % terms.length]);
        }
        withDictionary(docValues, (metadata, reader) -> { assertEveryValueReadsBack(docValues, reader); });
    }

    /** Values of no bytes are terms like any other, and repeat like any other. */
    public void testEmptyValuesAreTerms() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(400, 1200)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(d % 3 == 0 ? "" : (d % 3 == 1 ? "yes" : "no"));
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /** What a dictionary column records about itself survives the round trip through its metadata. */
    public void testMetadataRoundTrip() throws IOException {
        final String[] terms = { "GET", "POST", "PUT" };
        final BytesRef[] docValues = new BytesRef[between(300, 1200)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            final StringColumnMetadata read = roundTrip(metadata, docValues.length);
            assertEquals("layout", metadata.layout(), read.layout());
            assertEquals("dictionary size", dictionaryOf(metadata).dictionarySize(), dictionaryOf(read).dictionarySize());
            assertEquals("dictionary terms", dictionaryOf(metadata).dictionary().numValues(), dictionaryOf(read).dictionary().numValues());
            assertEquals("ordinals", dictionaryOf(metadata).ordinals().numValues(), dictionaryOf(read).ordinals().numValues());
            assertEquals("numValues", metadata.numValues(), read.numValues());
            assertEquals("escapes", dictionaryOf(metadata).escapes().numValues(), dictionaryOf(read).escapes().numValues());
        });
    }

    /** The same, over a column that escaped values, so the rank table is written and read too. */
    public void testMetadataRoundTripWithEscapes() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(500, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = d % 40 == 3 ? new BytesRef("rare-" + d) : new BytesRef(d % 2 == 0 ? "up" : "down");
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertTrue("some values escaped", dictionaryOf(metadata).hasEscapes());
            final StringColumnMetadata read = roundTrip(metadata, docValues.length);
            assertEquals("escapes", dictionaryOf(metadata).escapes().numValues(), dictionaryOf(read).escapes().numValues());
            assertEquals(
                "rank table length",
                dictionaryOf(metadata).escapeRanks().dataLength(),
                dictionaryOf(read).escapeRanks().dataLength()
            );
            assertEquals(
                "rank table offset",
                dictionaryOf(metadata).escapeRanks().dataOffset(),
                dictionaryOf(read).escapeRanks().dataOffset()
            );
        });
    }

    private static StringColumnMetadata roundTrip(final StringColumnMetadata metadata, final int maxDoc) throws IOException {
        final byte[] buffer = new byte[1 << 16];
        final org.apache.lucene.store.ByteArrayDataOutput out = new org.apache.lucene.store.ByteArrayDataOutput(buffer);
        metadata.writeTo(out);
        final org.apache.lucene.store.ByteArrayDataInput in = new org.apache.lucene.store.ByteArrayDataInput(buffer, 0, out.getPosition());
        return StringColumnMetadata.readFrom(in, Math.max(maxDoc, 1), org.elasticsearch.columnar.FormatVersion.CURRENT);
    }

    /** A dictionary column's summary terms are its dictionary, so only the counts are stored beside it. */
    public void testDictionaryColumnKeepsASummary() throws IOException {
        final String[] terms = { "DEBUG", "ERROR", "INFO" };
        final BytesRef[] docValues = new BytesRef[600];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertTrue("kept a summary", reader.hasSummary());
            assertNull("terms are the dictionary", metadata.summary().terms());
            assertEquals("values the counts are a share of", docValues.length, reader.summaryValues());
            final List<BytesRef> summaryTerms = new ArrayList<>();
            final List<Long> counts = new ArrayList<>();
            reader.readSummary(summaryTerms, counts);
            assertEquals("one count per term", terms.length, summaryTerms.size());
            assertEquals(summaryTerms.size(), counts.size());
            long total = 0;
            for (Long count : counts) {
                total += count;
            }
            // Counts are the survey's, so lower bounds: they never claim more than the column holds.
            assertThat("counts never overstate", total, lessThanOrEqualTo((long) docValues.length));
        });
    }

    /**
     * A column that stayed plain keeps a summary too. The survey already ran, and the segment this one is
     * merged into may well be worth a dictionary even where this one was not.
     */
    public void testPlainColumnKeepsASummary() throws IOException {
        // Every term twice, so the vocabulary is complete but too large a share of the column to keep.
        final BytesRef[] docValues = new BytesRef[600];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef("value-" + (d / 2));
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.PLAIN, metadata.layout());
            assertTrue("kept a summary anyway", reader.hasSummary());
            assertNotNull("a plain column writes its summary terms", metadata.summary().terms());
            final List<BytesRef> summaryTerms = new ArrayList<>();
            final List<Long> counts = new ArrayList<>();
            reader.readSummary(summaryTerms, counts);
            assertEquals("one count per term", summaryTerms.size(), counts.size());
            assertThat("found the repeated terms", summaryTerms.size(), greaterThan(0));
        });
    }

    /** A column with nothing worth naming records no summary. */
    public void testAllDistinctValuesKeepNoSummary() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(200, 800)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef("id-" + d);
        }
        withDictionary(docValues, (metadata, reader) -> assertFalse("nothing repeats", reader.hasSummary()));
    }

    /** What a column recorded of its survey survives the round trip through its metadata. */
    public void testSummaryRoundTrip() throws IOException {
        final BytesRef[] docValues = new BytesRef[800];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = d % 40 == 3 ? new BytesRef("rare-" + d) : new BytesRef(d % 2 == 0 ? "up" : "down");
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertTrue("kept a summary", metadata.hasSummary());
            final StringColumnMetadata read = roundTrip(metadata, docValues.length);
            assertTrue("summary survives", read.hasSummary());
            assertEquals("counts offset", metadata.summary().countsOffset(), read.summary().countsOffset());
            assertEquals("counts length", metadata.summary().countsLength(), read.summary().countsLength());
            assertEquals("values", metadata.summary().numValues(), read.summary().numValues());
        });
    }

    /**
     * Escapes at the positions a rank is counted from: the first value, exactly on a block boundary, just
     * either side of one, and the last. An off-by-one in the base or the count between shows up here and
     * nowhere else.
     */
    public void testEscapesAtBlockBoundaries() throws IOException {
        final int block = StringColumnWriter.ESCAPE_RANK_BLOCK;
        final int size = block * 4;
        final int[] escapeAt = { 0, 1, block - 1, block, block + 1, 2 * block, size - 1 };
        final BytesRef[] docValues = withEscapesAt(size, escapeAt);
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertEquals("one escape per position", escapeAt.length, (int) dictionaryOf(metadata).escapes().numValues());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /** Every value in one block escaping, so a later block's base is offset by a whole block of them. */
    public void testAWholeBlockEscapes() throws IOException {
        final int block = StringColumnWriter.ESCAPE_RANK_BLOCK;
        final int size = block * 3;
        final int[] escapeAt = new int[block];
        for (int i = 0; i < block; i++) {
            escapeAt[i] = block + i;
        }
        final BytesRef[] docValues = withEscapesAt(size, escapeAt);
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /** A dictionary of one term: the escape marker is ordinal one, the narrowest it can be. */
    public void testDictionaryOfOneTerm() throws IOException {
        final BytesRef[] docValues = new BytesRef[600];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = d % 40 == 7 ? new BytesRef("odd-" + d) : new BytesRef("same");
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertEquals("one term", 1, dictionaryOf(metadata).dictionarySize());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /**
     * Dictionary sizes either side of a byte: the escape marker is one past the last ordinal, so at 255
     * terms it is the first value that no longer fits where every ordinal did.
     */
    public void testDictionarySizesAroundAByte() throws IOException {
        for (int terms : new int[] { 254, 255, 256, 257 }) {
            // Enough values per term that the whole vocabulary fits the budget a column of this size allows.
            final BytesRef[] docValues = new BytesRef[terms * 20];
            for (int d = 0; d < docValues.length; d++) {
                docValues[d] = new BytesRef("t" + (d % terms));
            }
            withDictionary(docValues, (metadata, reader) -> {
                assertEquals("layout at " + terms + " terms", StringColumnLayout.DICTIONARY, metadata.layout());
                assertEquals("dictionary size", terms, dictionaryOf(metadata).dictionarySize());
                assertEveryValueReadsBack(docValues, reader);
            });
        }
    }

    /** A column of {@code size} values over a few repeated terms, with a value seen once at each given position. */
    private static BytesRef[] withEscapesAt(int size, int[] positions) {
        final String[] terms = { "alpha", "bravo", "charlie", "delta" };
        final BytesRef[] docValues = new BytesRef[size];
        for (int d = 0; d < size; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        for (int at : positions) {
            docValues[at] = new BytesRef("escape-" + at);
        }
        return docValues;
    }

    private void withDictionary(final BytesRef[] docValues, final ColumnCheck check) throws IOException {
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, check);
    }

    private static void assertEveryValueReadsBack(final BytesRef[] docValues, final StringColumnReader reader) throws IOException {
        final List<BytesRef> expected = new ArrayList<>();
        for (BytesRef value : docValues) {
            if (value != null) {
                expected.add(value);
            }
        }
        final ColumnIterator iterator = reader.iterator();
        int seen = 0;
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            assertEquals("value at doc " + doc, docValues[doc], reader.valueAt(reader.firstValueAddress(iterator.rank())));
            seen++;
        }
        assertEquals("documents with a value", expected.size(), seen);
    }
}
