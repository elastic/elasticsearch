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
import java.util.Collections;
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

    /**
     * A column mostly made of nulls whose values are all one of a handful of terms. A null is named by a
     * reserved ordinal rather than by a dictionary entry, so it is no part of what a dictionary could
     * cover — and a column whose every actual value the dictionary names should take one however many of
     * its slots hold nothing.
     */
    public void testNullSlotsDoNotCostTheDictionary() throws IOException {
        final String[] terms = { "DEBUG", "ERROR", "INFO", "TRACE", "WARN" };
        final BytesRef[][] docSlots = new BytesRef[between(500, 3000)][];
        for (int d = 0; d < docSlots.length; d++) {
            // Four slots in five hold nothing, which is well past the coverage the policy asks for if the
            // nulls are counted against it.
            docSlots[d] = new BytesRef[] { d % 5 == 0 ? new BytesRef(terms[(d / 5) % terms.length]) : null };
        }
        withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, (metadata, reader) -> {
            assertTrue("expected null slots", metadata.hasNullSlots());
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertEquals("one ordinal per distinct term", terms.length, dictionaryOf(metadata).dictionarySize());
            assertFalse("nothing should have escaped", dictionaryOf(metadata).hasEscapes());
            for (int d = 0; d < docSlots.length; d++) {
                assertEquals("doc [" + d + "]", docSlots[d][0], reader.valueAt(reader.firstValueAddress(d)));
            }
        });
    }

    /**
     * What the ordinal space is, stated rather than assumed. Several places size a table over the terms and
     * rely on the escape marker landing one past its end — {@code ordinalMap} on the merge path turns an
     * escape away by letting it index off the end of an array sized for the terms. That only works while the
     * reserved ordinals sit exactly where they do, and nothing about the arithmetic says so out loud.
     */
    public void testTheOrdinalSpaceIsWhatEveryTableIsSizedAgainst() throws IOException {
        final String[] terms = { "DEBUG", "ERROR", "INFO", "TRACE", "WARN" };
        final BytesRef[][] docSlots = new BytesRef[between(400, 1200)][];
        for (int d = 0; d < docSlots.length; d++) {
            docSlots[d] = new BytesRef[] { switch (d % 11) {
                // Rare enough to escape a dictionary built from what the column repeats.
                case 5 -> new BytesRef("escaped-" + d);
                case 7 -> null;
                default -> new BytesRef(terms[d % terms.length]);
            } };
        }
        withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, (metadata, reader) -> {
            final StringColumnMetadata.Dictionary dictionary = dictionaryOf(metadata);
            final DictionaryStringColumnReader column = (DictionaryStringColumnReader) reader;
            assertTrue("expected values to have escaped", dictionary.hasEscapes());
            assertTrue("expected null slots", metadata.hasNullSlots());

            // A null sorts below every term, and the escape marker sits one past the last of them, so the
            // terms are exactly the ordinals in between and a table sized over them ends at the marker.
            assertTrue(
                "the null is below the first term",
                StringColumnMetadata.Dictionary.NULL_ORDINAL < StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL
            );
            assertEquals(
                "the escape marker is one past the last term",
                dictionary.dictionarySize() + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL,
                dictionary.escapeOrdinal()
            );
            assertEquals("and the reader agrees", dictionary.escapeOrdinal(), column.escapeOrdinal());

            // Every ordinal the column actually stores is a term's, the null's, or the marker's - nothing else.
            int nulls = 0;
            int escapes = 0;
            int named = 0;
            for (int d = 0; d < docSlots.length; d++) {
                final int ordinal = column.ordinalAt(reader.firstValueAddress(d));
                final BytesRef value = reader.valueAt(reader.firstValueAddress(d));
                if (ordinal == StringColumnMetadata.Dictionary.NULL_ORDINAL) {
                    assertNull("doc [" + d + "] takes the null ordinal", value);
                    nulls++;
                } else if (ordinal == dictionary.escapeOrdinal()) {
                    assertNotNull("doc [" + d + "] escaped", value);
                    escapes++;
                } else {
                    assertTrue(
                        "doc [" + d + "] ordinal " + ordinal + " is in term space",
                        ordinal >= StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL && ordinal < dictionary.escapeOrdinal()
                    );
                    assertEquals("doc [" + d + "] resolves through its term", value, column.termAt(ordinal, new BytesRef()));
                    named++;
                }
                assertEquals("doc [" + d + "]", docSlots[d][0], value);
            }
            assertEquals("every slot accounted for", docSlots.length, nulls + escapes + named);
            assertThat("some slot took each of the three", nulls, greaterThan(0));
            assertThat("some slot escaped", escapes, greaterThan(0));
            assertThat("some slot was named", named, greaterThan(0));
            assertEquals("the escape count is what the column recorded", escapes, (int) reader.escapeCount());
            assertEquals("the null count is what the column recorded", nulls, (int) reader.numNullSlots());
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

    /**
     * Escaped values resolved out of the order they are stored in. Where an escaped value's bytes are is
     * counted forward from the value answered before it when that one is nearer, and from the start of its
     * block otherwise, so asking for an address below the last one is the only thing that takes the second
     * path. A miscount there returns another value's bytes rather than failing, which is the kind of wrong
     * nothing downstream would notice.
     */
    public void testEscapesResolvedOutOfOrder() throws IOException {
        final int block = StringColumnWriter.ESCAPE_RANK_BLOCK;
        final int size = block * 4;
        final int[] escapeAt = { 3, block - 2, block + 5, 2 * block + 1, 3 * block, size - 2 };
        final BytesRef[] docValues = withEscapesAt(size, escapeAt);
        withDictionary(docValues, (metadata, reader) -> {
            final DictionaryStringColumnReader dictionary = (DictionaryStringColumnReader) reader;
            final BytesRef scratch = new BytesRef();
            // Every document has a value here, so a document's rank is its value's address.
            final List<Integer> descending = new ArrayList<>();
            for (int at : escapeAt) {
                descending.add(at);
            }
            Collections.reverse(descending);
            for (int at : descending) {
                assertEquals(
                    "escape at " + at + " resolved descending",
                    docValues[at].utf8ToString(),
                    dictionary.resolveEscape(reader.firstValueAddress(at), scratch).utf8ToString()
                );
            }
            // And interleaved, so the cursor sits behind the address as often as ahead of it.
            for (int i = 0; i < escapeAt.length; i++) {
                final int at = escapeAt[i % 2 == 0 ? escapeAt.length - 1 - i / 2 : i / 2];
                assertEquals(
                    "escape at " + at + " resolved out of order",
                    docValues[at].utf8ToString(),
                    dictionary.resolveEscape(reader.firstValueAddress(at), scratch).utf8ToString()
                );
            }
            // Asking twice for the same address must not move the answer either.
            for (int at : escapeAt) {
                dictionary.resolveEscape(reader.firstValueAddress(at), scratch);
                assertEquals(
                    "escape at " + at + " resolved twice",
                    docValues[at].utf8ToString(),
                    dictionary.resolveEscape(reader.firstValueAddress(at), scratch).utf8ToString()
                );
            }
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

    /**
     * A multi-valued column that takes the dictionary. How a document's slots are found is decided apart from
     * how its values are named, so both have to hold at once: the ordinals address slots one-for-one while the
     * value-address table addresses documents, and a null slot is named like any other value while the
     * null-slot table is what still tells it from the empty term beside it.
     */
    public void testMultiValuedColumnTakesTheDictionary() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie", "" };
        final BytesRef[][] docSlots = new BytesRef[between(400, 1200)][];
        for (int d = 0; d < docSlots.length; d++) {
            final BytesRef[] slots = new BytesRef[between(1, 5)];
            for (int s = 0; s < slots.length; s++) {
                // A null now and then, and a value the dictionary will not name now and then, so the two
                // ways a slot leaves the ordinals both happen alongside multi-valued documents.
                if (slots.length > 1 && d % 11 == s) {
                    continue;
                }
                slots[s] = d % 17 == s ? new BytesRef("escape-" + d + "-" + s) : new BytesRef(terms[(d + s) % terms.length]);
            }
            if (slots.length == 1 && slots[0] == null) {
                slots[0] = new BytesRef(terms[d % terms.length]);
            }
            docSlots[d] = slots;
        }
        withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, (metadata, reader) -> {
            assertTrue("expected a dictionary", reader.hasDictionary());
            assertTrue("expected a multi-valued column", metadata.multiValued());
            assertEquals("numValues counts slots", numValues(docSlots), reader.numValues());
            assertEquals("null slots recorded", numNullSlots(docSlots), metadata.numNullSlots());

            final ColumnIterator iterator = reader.iterator();
            int seen = 0;
            for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                final int rank = iterator.rank();
                assertEquals("slot count at doc " + doc, docSlots[doc].length, reader.valueCount(rank));
                final long first = reader.firstValueAddress(rank);
                for (int slot = 0; slot < docSlots[doc].length; slot++) {
                    final long address = first + slot;
                    if (docSlots[doc][slot] == null) {
                        assertTrue("doc " + doc + " slot " + slot + " is null", reader.isNullSlot(address));
                    } else {
                        assertFalse("doc " + doc + " slot " + slot + " is a value", reader.isNullSlot(address));
                        assertEquals("doc " + doc + " slot " + slot, docSlots[doc][slot], reader.valueAt(address));
                    }
                }
                seen++;
            }
            assertEquals("documents with a value", numDocsWithField(docSlots), seen);
        });
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
