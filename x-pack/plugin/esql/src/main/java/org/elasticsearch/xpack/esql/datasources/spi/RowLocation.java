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
     * @param row            the reader's own record count at the failure, 1-based and local to what this reader
     *                       read. Not a count of ACCEPTED records: CSV counts a record it then rejects, so two
     *                       failures in a row render as 2 and 3; NDJSON un-counts a record dropped for exceeding
     *                       {@code external_max_record_size}, so a later record re-uses the ordinal; and on the CSV
     *                       reader's buffered paths a record that fails while being read is numbered ahead of the
     *                       good records of its batch, so the count can be short by up to a batch.
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
        // Any negative takes the unknown clause, not BYTE_UNKNOWN alone: the clause says only that no byte can be
        // named, which is true of every negative. Asserting BYTE_UNKNOWN instead would turn a cosmetic uncertainty on
        // an error path into a fatal failure under -ea.
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
     * The escape alone, with no cap, for store-supplied text printed inside brackets: a whole source location, or a
     * column name.
     * <p>
     * A location is not capped because it is what tells two objects sharing a basename apart, and the segment that
     * does so -- a region, a date, a partition key -- sits wherever the layout puts it, inside the window a fixed cut
     * removes. The warning text is its own deduplication key, so a collision deletes a message rather than shortening
     * it. The cost is that nothing bounds its width: neither {@code http.max_warning_header_size} nor
     * {@code SkipWarnings#MAX_ADDED_WARNINGS} caps one warning's length.
     * <p>
     * A key legitimately holding a bracket -- {@code part[0].ndjson} -- renders escaped too, as do the brackets of an
     * IPv6 authority: telling those apart would mean trusting a parse of the text being protected.
     */
    public static String escapedForDisplay(String storeSuppliedText) {
        return storeSuppliedText == null ? "" : escapeImpersonatingCharacters(storeSuppliedText);
    }

    private static String shorten(String objectName) {
        String displayable = escapeImpersonatingCharacters(objectName);
        if (displayable.length() <= MAX_OBJECT_NAME_CHARS) {
            return displayable;
        }
        // Cut on the name and escape the two ends, so a cut never lands inside a six-character escape and leaves a
        // stump that spells nothing, and the escape is counted against the cap. charsEscapingWithin also gives up a
        // surrogate half orphaned by either cut: a lone surrogate encodes to no UTF-8 sequence, so keeping it spends a
        // position in the cap to say nothing.
        int remaining = MAX_OBJECT_NAME_CHARS - CUT_MARKER.length();
        int headBudget = remaining / 2;
        int head = ErrorExcerpts.charsEscapingWithin(objectName, headBudget, true);
        int tail = ErrorExcerpts.charsEscapingWithin(objectName, remaining - headBudget, false);
        return escapeImpersonatingCharacters(objectName.substring(0, head)) + CUT_MARKER + escapeImpersonatingCharacters(
            objectName.substring(objectName.length() - tail)
        );
    }

    /**
     * Replaces every character a name could use to impersonate the text around it with a backslash-u escape of its
     * code point, six ASCII characters for one. (Spelled out, because a backslash followed by {@code u} is a unicode
     * escape anywhere in a Java source file, comments included.)
     * <p>
     * A control character, because the node log writes a message as it stands, so a key holding a newline -- which
     * S3 permits -- would counterfeit a log line; {@code HeaderWarning} encodes a newline but passes tab and U+0085.
     * A square bracket, because the clause is concatenated: {@code v.csv] counted from byte [12} would otherwise
     * render exactly what a byte-12 read of {@code v.csv} renders, and deduplication would keep the forgery.
     * <p>
     * The backslash is not escaped, so a name holding a bracket's six-character escape renders like one holding the
     * bracket. Escaping it would make this not idempotent, and {@link #of} applies it again to a reduced name.
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
