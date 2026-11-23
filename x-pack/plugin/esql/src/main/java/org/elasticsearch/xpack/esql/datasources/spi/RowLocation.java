/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Strings;

/**
 * The "where" clause a text reader's per-row message carries: which object, and which row of it.
 * <p>
 * A row-shaped reader is constructed per split and counts the records of its own input, so its ordinal is the
 * file's only for a split starting at byte zero. Making it file-global would mean counting records from byte
 * zero every time, which is the work splitting exists to avoid, so the message names the window the ordinal was
 * counted in and the reader of it starts from there.
 * <p>
 * Neither clause identifies the read uniquely. {@link StoragePath#objectName()} returns the last path segment
 * and never the full URI, so objects of a glob sharing a basename share a token; and the window clause tells two
 * chunks of one object apart only where each names its own start byte, every split passing {@link #BYTE_UNKNOWN}
 * rendering the same {@code counted within a split}. Where two failing rows render one string,
 * {@code InformationalWarningBudget} deduplicates by exact string and the user is shown one warning.
 */
public final class RowLocation {

    /**
     * Per-name character cap. A basename is bounded only by the store's key limit (1 KiB for S3) and this clause
     * travels to the client as a {@code Warning} response header once per failing row, so an uncut name blows the
     * response up as an uncut value would. 64 characters covers an ordinary filename whole; a longer one keeps
     * both ends, so the extension survives.
     */
    public static final int MAX_OBJECT_NAME_CHARS = 64;

    /**
     * Marks where an over-long name was cut. ASCII rather than {@code …}, because this clause travels as a
     * {@code Warning} header and {@code HeaderWarning}'s {@code doesNotNeedEncoding} set is {@code 1 + 0xFF} wide,
     * so a single-character ellipsis goes out as {@code %E2%80%A6}.
     */
    private static final String CUT_MARKER = "...";

    /**
     * Passed as {@code readStartByte} by a reader that cannot vouch for a file-global byte: a block-aligned split
     * of a splittable compressed object, whose offset is a boundary in the compressed file while the ordinal
     * counts decompressed records, and which has discarded a leading partial record the previous split reported.
     * The clause then says {@code counted within a split} rather than naming a byte the reader of the message
     * cannot tell is wrong. It is not withheld silently: a bare {@code row [N] of [object]} is what a read from
     * byte zero renders, so it reads as the file's own ordinal.
     */
    public static final long BYTE_UNKNOWN = -1L;

    private RowLocation() {}

    /**
     * Renders {@code row [N] of [object]}, plus {@code counted from byte [B] of the decoded stream} for a positive
     * {@code B} or
     * {@code counted within a split} for {@link #BYTE_UNKNOWN}. Zero takes neither, being a read from the start of
     * the file. An empty {@code objectName} leaves the object clause out rather than printing {@code of []}.
     *
     * @param row            how many records this reader has begun to read from its own input, 1-based. Not a
     *                       count of ACCEPTED records: CSV counts a record it then rejects, so two failures in a
     *                       row render as 2 and 3; and NDJSON un-counts a record dropped for exceeding
     *                       {@code external_max_record_size}, so a later record re-uses the ordinal.
     * @param objectName     the object as messages name it, already reduced to its displayable part
     * @param readStartByte  file-global byte offset of the first byte this reader was given, on the same axis as
     *                       its records: for compressed input, an offset into the decompressed stream. A
     *                       splittable compressed macro-split (bzip2, indexed zstd) has its offset on the
     *                       COMPRESSED axis, and neither reader translates one: NDJSON passes
     *                       {@link #BYTE_UNKNOWN}, and CSV passes it too except for the object's own first
     *                       split, which it passes as zero. Both render no byte, so no caller reaches here with
     *                       a cross-axis value.
     */
    public static String of(long row, String objectName, long readStartByte) {
        return new StringBuilder("row [").append(row)
            .append(']')
            .append(objectClause(" of", displayObjectName(objectName)))
            .append(countedIn(readStartByte))
            .toString();
    }

    /**
     * The bracketed object part of a message. {@code leadIn} carries the preposition ({@code " of"},
     * {@code " for"}, or empty where the sentence supplies the word) so the whole phrase disappears together when
     * the name is unavailable, rather than leaving a dangling preposition or an empty {@code []}.
     * <p>
     * One owner, because every message naming an object has to agree about how. The name is expected to have been
     * reduced by {@link #displayObjectName} already, so this neither caps nor escapes.
     */
    public static String objectClause(String leadIn, String objectName) {
        return objectName == null || objectName.isEmpty() ? "" : leadIn + " [" + objectName + "]";
    }

    /**
     * The window clause on its own, for a message reporting an AGGREGATE rather than one row.
     * <p>
     * An error-budget failure says how many rows it read before giving up, and that count is split-relative for
     * the same reason an ordinal is, so {@code [N] errors in [M] rows of [object]} would read as a statement about
     * the whole object. Sharing the rendering also keeps a detail and the summary above it from implying different
     * windows. Empty for a read from the start of the file, whose counts ARE the file's.
     * <p>
     * The byte names its axis because on a stream-compressed object it is a cumulative offset into the DECOMPRESSED
     * stream, which exceeds the stored object's length, so a user following it bare seeks past the end of the
     * {@code .gz}. Said unconditionally: the claim holds for a plain file too, and the read context carries no
     * compression signal to branch on.
     */
    public static String countedIn(long readStartByte) {
        // Any negative takes the unknown clause, not BYTE_UNKNOWN alone, and deliberately: the clause says only that
        // this message cannot name a byte, which is true of every negative a caller could arrive at, including one it
        // arrived at by arithmetic it got wrong. Narrowing it to BYTE_UNKNOWN would mean asserting here, and an
        // assertion in a rendering helper turns a cosmetic uncertainty on an error path into a fatal failure under
        // -ea -- which is the thing this clause is part of a change to stop doing.
        if (readStartByte > 0L) {
            return " counted from byte [" + readStartByte + "] of the decoded stream";
        }
        return readStartByte < 0L ? " counted within a split" : "";
    }

    /**
     * Reduces an object name to the token every message must use for it — escaped per
     * {@link #escapeImpersonatingCharacters}, then cut to {@link #MAX_OBJECT_NAME_CHARS} keeping both ends.
     * <p>
     * A reader applies this where it assigns its message-location field, so the summary warning and the details
     * under it carry the same token; capping only inside {@link #of} would render one long name two ways in one
     * response, which reads as two objects. {@link #of} applies it again, idempotently, so a caller that has not
     * cannot emit an uncut or unescaped name.
     * <p>
     * Two names agreeing on the kept head and tail reduce to one token, and exact-string deduplication then keeps
     * only the first. That is the shared-basename collision above reached through the cap, and accepted for the
     * same reason: the alternative is an unbounded name in a response header.
     * <p>
     * For a bare object name only. A whole location goes through {@link #escapedForDisplay}, which escapes without
     * capping.
     */
    public static String displayObjectName(String objectName) {
        return objectName == null ? "" : shorten(objectName);
    }

    /**
     * The escape alone, for store-supplied text about to be printed inside brackets, and deliberately no cap. A
     * whole source location is the main caller; a column name is the other.
     * <p>
     * A location is the string that RESOLVES the shared-basename collision {@link #displayObjectName} accepts, so
     * capping it destroys the one distinction it is printed for: the discriminating segment of a path sits
     * wherever the layout puts it — a region, a date, a partition key — which is inside the window a fixed-width
     * cut removes, and the warning text is itself the deduplication key, so the collision deletes a message rather
     * than shortening one.
     * <p>
     * The cost is that nothing then bounds the width. A key may be any length the store permits, each escaped
     * character costs six in the warning text and more on the wire, and nothing downstream re-bounds it:
     * {@code http.max_warning_header_size} and {@code http.max_warning_header_count} both default to unbounded,
     * and {@code SkipWarnings#MAX_ADDED_WARNINGS} caps the NUMBER of distinct warnings rather than any one's
     * length. Bounding a location without losing its discriminator needs a cut that keeps the discriminating
     * segment, which is a policy owed to every message embedding a store-supplied path.
     * <p>
     * A key legitimately holding a bracket — {@code part[0].ndjson} is an ordinary name — therefore renders
     * escaped, so what the message prints is not a string the store will accept back. A location whose authority is an IPv6 literal
     * pays the same on brackets the URI grammar contributed: {@code https://[2001:db8::1]/b/x} renders ten
     * characters wider. Telling those apart would mean trusting a parse of the very text being protected.
     */
    public static String escapedForDisplay(String storeSuppliedText) {
        return storeSuppliedText == null ? "" : escapeImpersonatingCharacters(storeSuppliedText);
    }

    private static String shorten(String objectName) {
        String displayable = escapeImpersonatingCharacters(objectName);
        if (displayable.length() <= MAX_OBJECT_NAME_CHARS) {
            return displayable;
        }
        int remaining = MAX_OBJECT_NAME_CHARS - CUT_MARKER.length();
        int head = remaining / 2;
        int tailStart = displayable.length() - (remaining - head);
        // Either cut can land between the halves of a surrogate pair. One step each way is enough for a name
        // decoded from UTF-8. The orphan is given up rather than kept: a lone surrogate encodes to no UTF-8
        // sequence, so it cannot show the character the name had there, and keeping it spends a position in the
        // cap to say nothing.
        if (head > 0 && Character.isHighSurrogate(displayable.charAt(head - 1))) {
            head--;
        }
        if (tailStart < displayable.length() && Character.isLowSurrogate(displayable.charAt(tailStart))) {
            tailStart++;
        }
        return displayable.substring(0, head) + CUT_MARKER + displayable.substring(tailStart);
    }

    /**
     * Replaces every character a name could use to impersonate the text around it with a backslash-u escape of its
     * code point — six ASCII characters where the original was one. The escape is spelled out in words rather than
     * written, because a backslash followed by {@code u} is a unicode escape anywhere in a Java source file,
     * comments included.
     * <p>
     * A control character, because the readers hand this clause to the node log beside the client warning, and the
     * console and legacy log layouts write a message as it stands — so a key holding a newline, which S3 permits,
     * counterfeits a log line. The response channel is safe against U+000A either way, {@code HeaderWarning.encode}
     * percent-encoding it, but its unencoded set keeps tab and U+0080 through U+00FF, of which U+0085 is a line
     * break. The predicate is {@code Character.isISOControl}, so U+2028 and U+2029 stay raw: a consumer breaking
     * lines on those can still be fed a forged one through the log.
     * <p>
     * A square bracket, because the clause is built by concatenation, so a name is free to close its own bracket
     * and open the next field: an object called {@code v.csv] counted from byte [12} renders character for
     * character what a byte-12 read of {@code v.csv} renders, and {@code InformationalWarningBudget} then
     * deduplicates the forgery against the real line. Both brackets are escaped, so the rule is "a bracket in a
     * name is never a bracket in the message".
     * <p>
     * Escaping is one-way, which bounds what it buys: a name holding the six characters of a bracket's escape
     * renders as one holding the bracket. Escaping the backslash too would close that, and is declined because
     * this method's output would then hold an escapable character while {@link #of} applies it a second time over
     * an already-reduced name — the idempotence {@code RowLocationTests} pins.
     * <p>
     * Applied before the cap, so {@link #MAX_OBJECT_NAME_CHARS} bounds what is emitted rather than what was read.
     */
    private static String escapeImpersonatingCharacters(String objectName) {
        int firstEscapable = -1;
        for (int i = 0; i < objectName.length(); i++) {
            if (impersonates(objectName.charAt(i))) {
                firstEscapable = i;
                break;
            }
        }
        if (firstEscapable < 0) {
            return objectName;
        }
        StringBuilder escaped = new StringBuilder(objectName.length() + 8).append(objectName, 0, firstEscapable);
        for (int i = firstEscapable; i < objectName.length(); i++) {
            char c = objectName.charAt(i);
            if (impersonates(c)) {
                escaped.append(Strings.format("\\u%04X", (int) c));
            } else {
                escaped.append(c);
            }
        }
        return escaped.toString();
    }

    /**
     * The characters {@link #escapeImpersonatingCharacters} rewrites, in one predicate so the scan and the
     * rewrite cannot disagree about the set.
     */
    private static boolean impersonates(char c) {
        return Character.isISOControl(c) || c == '[' || c == ']';
    }

    /**
     * How many characters {@link #escapedForDisplay} renders {@code c} as: six for one it escapes, one for
     * anything else. {@link ErrorExcerpts} uses it to bound escaped text without building it first.
     */
    static int escapedWidth(char c) {
        return impersonates(c) ? 6 : 1;
    }

    /**
     * {@link #of} with the column clause the per-cell messages carry: {@code row [N] of [object], column [c]}.
     * The column goes after the location so the two kinds of message share a prefix and sort together.
     */
    public static String of(long row, String objectName, long readStartByte, String column) {
        return of(row, objectName, readStartByte) + ", column [" + escapedForDisplay(column) + "]";
    }
}
