/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The ordinal pairs a dictionary query hands a {@link StringColumnReader.SlotWindow}, which decide what the
 * approximation admits before any slot is read. Pure arithmetic over ordinals, so no column is built here.
 */
public class DictionaryWindowRangesTests extends ESTestCase {

    private static final int ESCAPE_ORDINAL = 9;

    public void testARunOfTermsWithNoEscapes() throws Exception {
        assertArrayEquals(new long[] { 2, 4 }, DictionaryStringColumnReader.windowRanges(2, 5, ESCAPE_ORDINAL, false));
    }

    public void testARunOfTermsAndTheEscapes() throws Exception {
        assertArrayEquals(new long[] { 2, 4, 9, 9 }, DictionaryStringColumnReader.windowRanges(2, 5, ESCAPE_ORDINAL, true));
    }

    public void testOneTerm() throws Exception {
        assertArrayEquals(new long[] { 3, 3 }, DictionaryStringColumnReader.windowRanges(3, 4, ESCAPE_ORDINAL, false));
    }

    public void testNoTermMatchesSoOnlyTheEscapes() throws Exception {
        // NOTE: a term the dictionary does not hold leaves an empty run, and an escaped value may carry it.
        assertArrayEquals(new long[] { 9, 9 }, DictionaryStringColumnReader.windowRanges(3, 3, ESCAPE_ORDINAL, true));
    }

    public void testNothingCanMatchIsTheCallersToAnswer() {
        final AssertionError thrown = expectThrows(
            AssertionError.class,
            () -> DictionaryStringColumnReader.windowRanges(3, 3, ESCAPE_ORDINAL, false)
        );
        assertThat(thrown.getMessage(), containsString("nothing can match"));
    }

    public void testThePairsAdmitExactlyTheOrdinalsExpected() {
        for (int i = 0; i < 200; i++) {
            final int lowOrdinal = between(StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL, 40);
            final int highOrdinal = between(lowOrdinal, 40);
            final int escapeOrdinal = 41;
            final boolean escapesCanMatch = lowOrdinal == highOrdinal || randomBoolean();

            final SortedSet<Long> expected = new TreeSet<>();
            for (long ordinal = lowOrdinal; ordinal < highOrdinal; ordinal++) {
                expected.add(ordinal);
            }
            if (escapesCanMatch) {
                expected.add((long) escapeOrdinal);
            }

            final long[] ranges = DictionaryStringColumnReader.windowRanges(lowOrdinal, highOrdinal, escapeOrdinal, escapesCanMatch);
            assertEquals("pairs", 0, ranges.length % 2);
            final SortedSet<Long> admitted = new TreeSet<>();
            final List<String> pairs = new ArrayList<>();
            for (int r = 0; r < ranges.length; r += 2) {
                pairs.add("[" + ranges[r] + "," + ranges[r + 1] + "]");
                assertThat("a window is never handed a pair that sorts backwards", ranges[r], lessThanOrEqualTo(ranges[r + 1]));
                for (long ordinal = ranges[r]; ordinal <= ranges[r + 1]; ordinal++) {
                    admitted.add(ordinal);
                }
            }
            assertEquals("[" + lowOrdinal + "," + highOrdinal + ") escapes=" + escapesCanMatch + " gave " + pairs, expected, admitted);
        }
    }

}
