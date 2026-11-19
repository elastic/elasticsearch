/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

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
     * A reader that cannot vouch for a file-global byte passes {@link RowLocation#BYTE_UNKNOWN}, and the byte is
     * withheld rather than guessed, because the reader of a wrong byte has no way to tell it is wrong. The clause
     * still has to say that the ordinal was counted inside a split: dropping it altogether would render the same
     * text as a read from byte zero, which is the file's own ordinal, so the message would go back to claiming a
     * file position it does not have.
     */
    public void testAnUnknownByteStillSaysTheOrdinalIsCountedInASplit() {
        assertThat(RowLocation.of(5, "v.csv", RowLocation.BYTE_UNKNOWN), equalTo("row [5] of [v.csv] counted within a split"));
        assertThat(
            RowLocation.of(5, "v.csv", RowLocation.BYTE_UNKNOWN, "id"),
            equalTo("row [5] of [v.csv] counted within a split, column [id]")
        );
        assertThat(RowLocation.of(5, "v.csv", -42L), equalTo("row [5] of [v.csv] counted within a split"));
        assertThat(
            "an unknown byte must not read as a read from the start of the file",
            RowLocation.of(5, "v.csv", RowLocation.BYTE_UNKNOWN),
            not(equalTo(RowLocation.of(5, "v.csv", 0L)))
        );
    }

    /**
     * The readers hand this whole clause to {@code logger.warn} as well as to the client warning, so an object name
     * is the one piece of store-supplied text in a node log line. An S3 key may hold a newline, and the console and
     * legacy log layouts write a message as it stands, so an unescaped one would let a key counterfeit a log line.
     */
    public void testAControlCharacterInAnObjectNameCannotForgeALogLine() {
        String clause = RowLocation.of(3, "a\n[2026-11-17T00:00:00,000][WARN ][o.e.n.Node] forged.csv", 0L);
        assertThat("no raw newline may survive into a log line", clause, not(containsString("\n")));
        assertThat("no raw carriage return either", clause, not(containsString("\r")));
        assertThat("the character is named rather than dropped", clause, containsString("000A"));
        assertThat("the rest of the name is untouched", clause, containsString("forged.csv"));
        assertThat(RowLocation.of(3, "a\u0000b.csv", 0L), equalTo("row [3] of [a\\u0000b.csv]"));
    }

    /**
     * The window clause is reachable on its own because the error-budget failures report an aggregate and have
     * no ordinal to hang it on. Pinned here against the rendering inside a full clause so the summary a failed
     * read returns and the details it would have returned cannot describe different windows.
     */
    public void testTheWindowClauseRendersTheSameAloneAsItDoesInAFullClause() {
        assertThat("a read from the start of the file qualifies nothing", RowLocation.countedIn(0L), equalTo(""));
        assertThat(RowLocation.countedIn(4194304L), equalTo(" counted from byte [4194304]"));
        assertThat(RowLocation.countedIn(RowLocation.BYTE_UNKNOWN), equalTo(" counted within a split"));
        for (long startByte : new long[] { 0L, 4194304L, RowLocation.BYTE_UNKNOWN }) {
            assertThat(
                "the aggregate and the per-row detail must agree about the window",
                RowLocation.of(7, "v.csv", startByte),
                equalTo("row [7] of [v.csv]" + RowLocation.countedIn(startByte))
            );
        }
    }

    /**
     * The clause is built by concatenation and prints the name inside brackets, so a name holding a bracket can
     * close its own field and open the next one. The forged text is not merely confusing: the warning budget
     * deduplicates by exact string, so a name that renders as another row's location makes that row's warning
     * disappear and the user is told about one failure where there were two.
     */
    public void testABracketInAnObjectNameCannotForgeALocation() {
        String forged = RowLocation.of(1, "v.csv] counted from byte [12", 0L);
        String honest = RowLocation.of(1, "v.csv", 12L);
        assertThat("a byte-zero read must not render as a byte-12 read of another object", forged, not(equalTo(honest)));
        assertThat(
            "the forged read starts at byte zero, so it carries no byte clause",
            forged,
            not(containsString("counted from byte [12]"))
        );
        assertThat("the bracket is named rather than dropped", forged, containsString("005D"));
        assertThat("the rest of the name survives", forged, containsString("v.csv"));
        assertThat(RowLocation.of(2, "a[b].csv", 0L), equalTo("row [2] of [a\\u005Bb\\u005D.csv]"));
    }

    /**
     * Escaping expands a control character into six, so it has to run before the cap — capping first would let a
     * name made of control characters grow past the bound afterwards, which is the bound the response header needs.
     */
    public void testEscapingCannotPushTheNamePastTheCap() {
        String clause = RowLocation.of(1, "\n".repeat(20) + ".csv", 0L);
        assertThat(clause.length(), lessThanOrEqualTo("row [1] of []".length() + RowLocation.MAX_OBJECT_NAME_CHARS));
        assertThat(clause, not(containsString("\n")));
    }

    /**
     * Either cut falls on an arbitrary UTF-16 index and so can land between the halves of a surrogate pair. The
     * property that matters is downstream: the clause travels as UTF-8 on both channels, and an unpaired surrogate
     * encodes to no UTF-8 sequence, so a clause carrying one does not survive the trip unchanged.
     */
    public void testNeitherCutLeavesAnUnpairedSurrogate() {
        String grin = "\uD83D\uDE00";
        // The head cut keeps [0, 30), so a pair at 29-30 would be halved by it.
        assertSurvivesUtf8(RowLocation.of(1, "a".repeat(29) + grin + "b".repeat(40), 0L));
        // The tail cut keeps the last 31 characters, so a pair at 40-41 of a 72-character name would be halved.
        assertSurvivesUtf8(RowLocation.of(1, "a".repeat(40) + grin + "b".repeat(30), 0L));
        // A pair wholly inside a kept end must come through as the character it is.
        assertThat(RowLocation.of(1, grin + "x".repeat(300) + grin + ".csv", 0L), containsString(grin));
    }

    private void assertSurvivesUtf8(String clause) {
        assertThat(clause.length(), lessThanOrEqualTo("row [1] of []".length() + RowLocation.MAX_OBJECT_NAME_CHARS));
        assertThat(
            "an unpaired surrogate does not survive UTF-8, got: " + clause,
            new String(clause.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8),
            equalTo(clause)
        );
    }

    /**
     * A reader reduces the name once, where it assigns its message-location field, so the summary warning and the
     * details under it carry the same token; {@link RowLocation#of} reduces it again. The two passes must agree,
     * or a long or control-bearing name would render one way in the summary and another in the detail, which reads
     * as two different objects.
     * <p>
     * It holds only while no character the escape EMITS is itself escaped, so the bracket-bearing names here
     * guard the set as it stands: adding a backslash to it -- which would make the rendering injective, and is
     * declined on {@code RowLocation.escapeImpersonatingCharacters} for this reason -- fails this test rather
     * than quietly double-escaping every name that carries a bracket.
     */
    public void testReducingTheNameTwiceChangesNothing() {
        for (String name : new String[] {
            "v.csv",
            "p".repeat(300) + ".csv",
            "a\nb.csv",
            "\n".repeat(20) + ".csv",
            "a[b].csv",
            "v.csv] counted from byte [12",
            "a[" + "y".repeat(300) + "].csv" }) {
            String once = RowLocation.displayObjectName(name);
            assertThat(RowLocation.displayObjectName(once), equalTo(once));
            assertThat(RowLocation.of(1, once, 0L), equalTo(RowLocation.of(1, name, 0L)));
        }
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
