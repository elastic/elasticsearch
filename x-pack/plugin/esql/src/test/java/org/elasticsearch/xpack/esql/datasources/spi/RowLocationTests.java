/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class RowLocationTests extends ESTestCase {

    /**
     * The read that begins at byte zero counts the file's own rows, so its ordinal needs no qualifier. Printing one
     * anyway would put a word about splits in front of every message from every unsplit file, which is most of them.
     */
    public void testAReadFromByteZeroIsNotQualified() {
        assertThat(RowLocation.of(9, "v.csv", 0L), equalTo("row [9] of [v.csv]"));
        assertThat(RowLocation.of(9, "v.csv", 0L, "id"), equalTo("row [9] of [v.csv], column [id]"));
    }

    /**
     * Every later read counts from its own first byte, so the ordinal is not the file's and the message says which
     * byte it started from. This is the whole defect: the same physical row was described as row 9 and as row 5
     * depending on how the file happened to be divided, with nothing in either message to say so.
     */
    public void testALaterReadNamesTheByteItCountedFrom() {
        assertThat(RowLocation.of(5, "v.csv", 612L), equalTo("row [5] of [v.csv] counted from byte [612]"));
        assertThat(RowLocation.of(5, "v.csv", 612L, "id"), equalTo("row [5] of [v.csv] counted from byte [612], column [id]"));
    }

    /**
     * Two reads of one file that fail at the same ordinal must not render the same text.
     * {@code InformationalWarningBudget} deduplicates warnings by exact string, so identical text meant the second
     * failure was dropped with no overflow marker and no trace of it.
     */
    public void testSameOrdinalInDifferentReadsRendersDifferently() {
        assertThat(RowLocation.of(253, "a.json.gz", 0L), not(equalTo(RowLocation.of(253, "a.json.gz", 640491456L))));
        assertThat(RowLocation.of(253, "a.json.gz", 4194304L), not(equalTo(RowLocation.of(253, "a.json.gz", 8388608L))));
    }

    /**
     * {@link StoragePath#objectName()} returns an empty string when it cannot extract a name safely. The clause then
     * leaves the object out rather than printing an empty bracket pair, which says nothing and reads like a bug.
     */
    public void testAnUnavailableObjectNameIsOmittedRatherThanEmpty() {
        assertThat(RowLocation.of(3, "", 0L), equalTo("row [3]"));
        assertThat(RowLocation.of(3, null, 0L), equalTo("row [3]"));
        assertThat(RowLocation.of(3, "", 99L), equalTo("row [3] counted from byte [99]"));
    }

    /**
     * An object name is user-supplied and bounded only by the store's key limit, and this clause goes into a
     * {@code Warning} response header once per failing row. A name longer than the cap keeps both ends so the
     * extension — the part that says what was being read — survives the cut.
     */
    public void testAnOverLongObjectNameIsCutAtBothEnds() {
        String name = "p".repeat(300) + ".csv";
        String clause = RowLocation.of(1, name, 0L);
        assertThat(clause.length(), lessThanOrEqualTo("row [1] of []".length() + RowLocation.MAX_OBJECT_NAME_CHARS));
        assertThat("the head identifies the prefix", clause, containsString("ppp"));
        assertThat("the tail keeps the extension", clause, containsString(".csv]"));
        assertThat("the cut is marked", clause, containsString("..."));
        // A Warning response header percent-encodes outside ASCII, so a non-ASCII marker reaches the user as
        // an escape sequence. Nothing else in this clause may carry one either.
        assertTrue("the clause must be ASCII, got: " + clause, clause.chars().allMatch(c -> c < 128));
    }

    /** A name exactly at the cap is printed whole — the guard must not cut what fits. */
    public void testANameAtTheCapIsNotCut() {
        String name = "q".repeat(RowLocation.MAX_OBJECT_NAME_CHARS);
        assertThat(RowLocation.of(1, name, 0L), equalTo("row [1] of [" + name + "]"));
    }
}
