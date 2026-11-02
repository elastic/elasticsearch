/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;

/**
 * The one pass that counts a column and surveys it together must answer exactly as the two passes it
 * replaces did: the totals as the writer's counting loop computed them, and the terms as
 * {@link Vocabulary#survey} finds them over the same values. Both are deterministic over the same input, so
 * the comparison is exact rather than a property.
 */
public class VocabularyCountAndSurveyTests extends ColumnarStringTestCase {

    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);
    private static final SummaryPolicy ROOMY_SUMMARY = SummaryPolicy.sized(512 * 1024);

    /** The longest value a term table admits; anything longer is counted but never surveyed into a term. */
    private static final int MAX_TERM_BYTES = ByteBlockPool.BYTE_BLOCK_SIZE - 2;

    /** Random slots, with nulls and gaps, under a roomy policy that keeps everything it sees. */
    public void testRandomColumn() throws IOException {
        final BytesRef[][] docSlots = randomDocSlots(between(100, 1000), 5, randomBoolean(), randomBoolean());
        salt(docSlots);
        assertCountAndSurveyMatches(docSlots, ROOMY, ROOMY_SUMMARY);
    }

    /** One slot a document, every one a value: the shape {@code oneSlotADocument} is true for. */
    public void testSingleValuedColumn() throws IOException {
        final BytesRef[][] docSlots = randomDocSlots(between(100, 1000), 1, randomBoolean(), false);
        salt(docSlots);
        assertCountAndSurveyMatches(docSlots, ROOMY, ROOMY_SUMMARY);
    }

    /** A policy too tight for the column, so the survey evicts along the way; the merged pass must evict alike. */
    public void testUnderATightPolicy() throws IOException {
        final BytesRef[][] docSlots = randomDocSlots(between(500, 2000), 3, false, randomBoolean());
        salt(docSlots);
        final DictionaryPolicy tight = new DictionaryPolicy(64, 0.5, 1.0);
        assertCountAndSurveyMatches(docSlots, tight, SummaryPolicy.sized(64));
    }

    /** Values no term table holds are counted, in length and in number, but never become terms. */
    public void testValuesLongerThanATermIsAllowed() throws IOException {
        final BytesRef[][] docSlots = randomDocSlots(between(20, 60), 2, true, true);
        for (BytesRef[] slots : docSlots) {
            if (slots != null && randomBoolean()) {
                final byte[] bytes = new byte[MAX_TERM_BYTES + between(1, 32)];
                Arrays.fill(bytes, (byte) randomIntBetween('a', 'z'));
                slots[between(0, slots.length - 1)] = new BytesRef(bytes);
            }
        }
        assertCountAndSurveyMatches(docSlots, ROOMY, ROOMY_SUMMARY);
    }

    /** A column of nothing but nulls has slots and documents, no length, and nothing to survey. */
    public void testOnlyNulls() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(10, 100)][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            if (randomBoolean()) {
                docSlots[doc] = new BytesRef[between(1, 4)];
            }
        }
        if (numDocsWithField(docSlots) == 0) {
            docSlots[0] = new BytesRef[2];
        }
        final Vocabulary.Counted counted = Vocabulary.countAndSurvey(cursor(docSlots), ROOMY, ROOMY_SUMMARY);
        assertEquals(totals(docSlots), counted.totals());
        assertEquals("no length to report", -1, counted.totals().minLength());
        assertEquals("no length to report", -1, counted.totals().maxLength());
        assertNotNull("surveyed, even to nothing", counted.survey());
        assertNull("nothing worth naming", counted.survey().terms());
    }

    /** An empty column: no document holds a slot. */
    public void testEmptyColumn() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(1, 50)][];
        final Vocabulary.Counted counted = Vocabulary.countAndSurvey(cursor(docSlots), ROOMY, ROOMY_SUMMARY);
        assertEquals(new StringColumnValues.Totals(0, 0, 0, -1, -1, true), counted.totals());
        assertNull(counted.survey().terms());
    }

    /** Terms the dictionary must not credit: the empty string beside nulls, both zero bytes long. */
    public void testEmptyStringsBesideNulls() throws IOException {
        final BytesRef empty = new BytesRef("");
        final BytesRef[][] docSlots = new BytesRef[between(200, 800)][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = switch (doc % 4) {
                case 0 -> new BytesRef[] { empty };
                case 1 -> new BytesRef[] { null, new BytesRef("kept-" + (doc % 3)) };
                case 2 -> new BytesRef[] { empty, null };
                default -> new BytesRef[] { new BytesRef("kept-" + (doc % 3)) };
            };
        }
        assertCountAndSurveyMatches(docSlots, ROOMY, ROOMY_SUMMARY);
    }

    private static void assertCountAndSurveyMatches(BytesRef[][] docSlots, DictionaryPolicy policy, SummaryPolicy summaryPolicy)
        throws IOException {
        final Vocabulary.Counted counted = Vocabulary.countAndSurvey(cursor(docSlots), policy, summaryPolicy);
        assertEquals("totals", totals(docSlots), counted.totals());
        assertNotNull("a survey, whatever it found", counted.survey());

        final Vocabulary.Terms standalone = Vocabulary.survey(cursor(docSlots), policy, summaryPolicy);
        final Vocabulary.Terms combined = counted.survey().terms();
        assertEquals("both find a vocabulary or neither does", standalone == null, combined == null);
        if (standalone == null) {
            return;
        }
        assertEquals("coverage", standalone.coverage(), combined.coverage(), 0.0);
        assertEquals("column bytes", standalone.columnBytes(), combined.columnBytes());
        assertEquals("dictionary bytes", standalone.dictionaryBytes(), combined.dictionaryBytes());
        assertEquals("values", standalone.numValues(), combined.numValues());
        assertEquals("best coverage", standalone.bestCoverage(), combined.bestCoverage());
        assertSameTerms("dictionary", standalone, standalone.dictionaryIds(), combined, combined.dictionaryIds());
        assertSameTerms("summary", standalone, standalone.summaryIds(), combined, combined.summaryIds());
        for (int ordinal = 0; ordinal < standalone.dictionarySize(); ordinal++) {
            assertEquals("count of dictionary ordinal " + ordinal, standalone.countOf(ordinal), combined.countOf(ordinal));
        }
        for (int ordinal = 0; ordinal < standalone.summarySize(); ordinal++) {
            assertEquals("count of summary ordinal " + ordinal, standalone.summaryCountOf(ordinal), combined.summaryCountOf(ordinal));
        }
    }

    private static void assertSameTerms(
        String what,
        Vocabulary.Terms standalone,
        int[] standaloneIds,
        Vocabulary.Terms combined,
        int[] combinedIds
    ) {
        assertEquals(what + " size", standaloneIds.length, combinedIds.length);
        final BytesRef expected = new BytesRef();
        final BytesRef actual = new BytesRef();
        for (int ordinal = 0; ordinal < standaloneIds.length; ordinal++) {
            standalone.terms().get(standaloneIds[ordinal], expected);
            combined.terms().get(combinedIds[ordinal], actual);
            assertEquals(what + " term at ordinal " + ordinal, expected, actual);
        }
    }

    /**
     * Puts a few frequent terms among the random values, so the column holds something a dictionary names
     * as well as values held once, and the empty string among them.
     */
    private static void salt(BytesRef[][] docSlots) {
        final String[] common = { "alpha", "bravo", "charlie", "" };
        for (BytesRef[] slots : docSlots) {
            if (slots != null && randomBoolean()) {
                slots[between(0, slots.length - 1)] = new BytesRef(randomFrom(common));
            }
        }
    }
}
