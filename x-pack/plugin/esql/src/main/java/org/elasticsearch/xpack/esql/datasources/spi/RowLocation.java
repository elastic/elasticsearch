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
 * A row-shaped reader is constructed per split, counts from zero the records it begins to read, and so measures
 * distance into its own input rather than into the file. For the split that starts at byte zero the two are
 * the same thing and the ordinal is the file's; for every later split it is not, and a bare {@code row [N]}
 * then sends the reader of the message to a row that is well formed. The number cannot be made file-global
 * without counting records from byte zero of the file, which is the work splitting exists to avoid — so the
 * message states the window the ordinal was counted in instead, and the reader of it can find the row from
 * the window's start.
 * <p>
 * Naming the object also separates the detail lines of a glob read, which arrive interleaved in one response
 * under a single summary warning. It does not fully disambiguate them: {@link StoragePath#objectName(String)} is the
 * last path segment by design (it "never returns the full URI — fails closed rather than leaking a bucket or
 * prefix"), so a glob over objects that share a basename — {@code dt=…/part-00000.csv} under many prefixes,
 * the hive-partitioned norm — still names every one of them the same. The byte clause does not rescue that
 * case: two such objects each failing at the same ordinal of their FIRST split render the identical string,
 * because a read starting at byte zero carries no byte clause at all. What the byte clause does fix is the
 * case where the windows differ — two chunks of ONE object failing at the same ordinal, which is every gzip
 * read, since the stream is segmented and each segment's decoder counts from zero.
 * {@code InformationalWarningBudget} deduplicates by exact string, so those previously collapsed into a
 * single warning with nothing to say that a second record had failed. The same-basename collision survives
 * and is not addressed here.
 */
public final class RowLocation {

    /**
     * Per-name character cap. An object name is user-supplied text: a basename is bounded only by the store's key
     * limit (1 KiB for S3), and this clause travels to the client as a {@code Warning} response header once per
     * failing row, so an uncut name would blow the response up exactly as an uncut value would — the reason
     * {@link ErrorExcerpts} exists. Cut here, where the name enters the message. 64 characters covers an ordinary
     * filename whole; a longer one keeps both ends, so its extension survives, which is the part that identifies
     * what the reader was reading.
     */
    public static final int MAX_OBJECT_NAME_CHARS = 64;

    /**
     * Marks where an over-long name was cut. ASCII on purpose, unlike {@link ErrorExcerpts}'s {@code \u2026}:
     * this clause travels as a {@code Warning} response header, and {@code HeaderWarning.escapeAndEncode}
     * percent-encodes anything outside ASCII, so a single-character ellipsis is transmitted as {@code %E2%80%A6}.
     * Whether a given client renders that or decodes it back is the client's business, which is the reason to
     * stay inside ASCII rather than rely on it. Observed in a warning drained from a read.
     */
    private static final String CUT_MARKER = "...";

    /**
     * Passed as {@code readStartByte} by a reader that cannot vouch for a file-global byte for its own input —
     * a block-aligned split of a splittable compressed object, where the split offset is a boundary in the
     * compressed file while the ordinal counts decompressed records, and where the reader has also discarded a
     * leading partial record the previous split reported. The byte is then withheld — a wrong byte is a worse
     * instruction than none, since the reader of the message cannot tell it is wrong — and the clause says
     * {@code counted within a split} instead. Withholding it silently would not do: the message would fall back
     * to a bare {@code row [N] of [object]}, which is what a read from byte zero renders and so reads as the
     * file's own ordinal, reinstating for this arm exactly the mis-location the clause exists to end. What the
     * reader of the message can still do with it is bounded, and deliberately so: the ordinal locates the row
     * within one split of the object and no byte places that split, so it serves to tell two failures apart and
     * not to find the row in the file.
     */
    public static final long BYTE_UNKNOWN = -1L;

    private RowLocation() {}

    /**
     * Renders {@code row [N] of [object]}, with {@code counted from byte [B]} appended when {@code B} is
     * positive — i.e. when the reader's input does not begin at the start of the file, so its ordinal is not
     * the file's — and {@code counted within a split} when the reader passed {@link #BYTE_UNKNOWN}, which says
     * the same thing without a byte to anchor it. A zero takes neither clause, being a read from the start of
     * the file, whose ordinal IS the file's. An empty {@code objectName} (see {@link StoragePath#objectName()}, which returns one when the
     * name cannot be extracted safely) leaves the object clause out rather than printing {@code of []}.
     *
     * @param row            how many records this reader has begun to read from its own input, 1-based. Not a
     *                       count of ACCEPTED records: CSV counts a record it then rejects, so two failures in a
     *                       row render as 2 and 3; and NDJSON un-counts a record dropped for exceeding
     *                       {@code external_max_record_size}, so a later record re-uses the ordinal.
     * @param objectName     the object as messages name it, already reduced to its displayable part
     * @param readStartByte  file-global byte offset of the first byte this reader was given; for compressed
     *                       input it is an offset into the decompressed stream, which is the axis the
     *                       reader's own records sit on. A splittable compressed macro-split (bzip2, indexed
     *                       zstd) has a split offset on the COMPRESSED axis, and both readers pass
     *                       {@link #BYTE_UNKNOWN} for those rather than translating, so no caller reaches here
     *                       with a cross-axis value. {@code CsvFormatReader} carries the matching invariant for
     *                       {@code _rowPosition}, which is unaffected because the operator factory
     *                       null-splices that slot for a compressed-offset split.
     */
    public static String of(long row, String objectName, long readStartByte) {
        StringBuilder where = new StringBuilder("row [").append(row).append(']');
        if (objectName != null && objectName.isEmpty() == false) {
            where.append(" of [").append(shorten(objectName)).append(']');
        }
        if (readStartByte > 0L) {
            where.append(" counted from byte [").append(readStartByte).append(']');
        } else if (readStartByte < 0L) {
            where.append(" counted within a split");
        }
        return where.toString();
    }

    /**
     * Reduces an object name to the token every message must use for it. A reader applies this where it assigns
     * its message-location field, so the summary warning and the per-row details sitting under it carry the SAME
     * token: capping only inside {@link #of} would make a long name render two ways in one response, which reads
     * as two different objects. It also escapes the name's control characters, for the reason given on
     * {@link #escapeControlCharacters}. {@link #of} applies both again, idempotently — escaped text holds no
     * control character left to escape, and a name already inside the cap is returned as it stands — so a
     * caller that has not cannot emit an uncut or unescaped name. This is the one statement of that rationale;
     * call sites point here rather than repeat it.
     * <p>
     * Two names agreeing on the kept head and tail but differing in the cut middle reduce to one token, so
     * objects that would otherwise have been told apart can produce identical text and the exact-string
     * deduplication then keeps only the first. That is the same limitation as the shared-basename case this
     * class documents above, reached through the cap rather than through the path, and it is accepted for the
     * same reason: the alternative is an unbounded name in a response header.
     * <p>
     * Cuts to {@link #MAX_OBJECT_NAME_CHARS}, keeping both ends. Same head/tail shape as
     * {@link ErrorExcerpts#summarize}, without its character count: the count tells a user how much of a VALUE
     * they cannot see, which is actionable, whereas the length of a key they chose is not.
     */
    public static String displayObjectName(String objectName) {
        return objectName == null ? "" : shorten(objectName);
    }

    private static String shorten(String objectName) {
        String displayable = escapeControlCharacters(objectName);
        if (displayable.length() <= MAX_OBJECT_NAME_CHARS) {
            return displayable;
        }
        int remaining = MAX_OBJECT_NAME_CHARS - CUT_MARKER.length();
        int head = remaining / 2;
        int tailStart = displayable.length() - (remaining - head);
        // Both cuts fall on an arbitrary UTF-16 index, so either can land between the halves of a surrogate
        // pair and leave an unpaired one. Give the orphan up rather than keep it: a lone surrogate encodes to
        // no UTF-8 sequence, so whatever each channel substitutes for it, what it never shows is the character
        // the name had there -- half of which was cut away regardless. Keeping it spends a position in the cap
        // to say nothing. Dropping shortens the result, which the cap permits.
        if (head > 0 && Character.isHighSurrogate(displayable.charAt(head - 1))) {
            head--;
        }
        if (tailStart < displayable.length() && Character.isLowSurrogate(displayable.charAt(tailStart))) {
            tailStart++;
        }
        return displayable.substring(0, head) + CUT_MARKER + displayable.substring(tailStart);
    }

    /**
     * Replaces every control character in an object name with a backslash-u escape of its code point (a newline
     * becomes {@code 000A}), because this clause is
     * the one piece of an object's name that reaches the node log: the readers pass the whole rendered location
     * to {@code logger.warn} alongside the client warning, so that the operator reading the log and the user
     * reading the response see the same row of the same object. A name is store-supplied text — an S3 key may
     * hold any UTF-8, newline included, and this class's parser keeps the last path segment verbatim — and the
     * console and legacy log layouts write a message as it stands, so an embedded newline would let a key
     * counterfeit a second log line. The response channel is already safe and stays so:
     * {@code HeaderWarning.encode} leaves the C0 range out of its unencoded set, so a newline travels as
     * {@code %0A} with or without this.
     * <p>
     * Escaping is one-way and is not meant to be reversed: a name holding a literal backslash followed by
     * {@code u} renders the same as one holding the escape of that code point. The clause's job is to identify
     * which object a row came from, which survives the ambiguity; a name recovered from a log line is not
     * something to feed back to a store.
     * <p>
     * Applied before the cap, so {@link #MAX_OBJECT_NAME_CHARS} bounds what is actually emitted — an escape is
     * six characters where the control was one, and capping first would let a name of controls expand past the
     * bound afterwards.
     */
    private static String escapeControlCharacters(String objectName) {
        int firstControl = -1;
        for (int i = 0; i < objectName.length(); i++) {
            if (Character.isISOControl(objectName.charAt(i))) {
                firstControl = i;
                break;
            }
        }
        if (firstControl < 0) {
            return objectName;
        }
        StringBuilder escaped = new StringBuilder(objectName.length() + 8).append(objectName, 0, firstControl);
        for (int i = firstControl; i < objectName.length(); i++) {
            char c = objectName.charAt(i);
            if (Character.isISOControl(c)) {
                escaped.append(Strings.format("\\u%04X", (int) c));
            } else {
                escaped.append(c);
            }
        }
        return escaped.toString();
    }

    /**
     * {@link #of} with the column clause the per-cell messages carry: {@code row [N] of [object], column [c]}.
     * The column goes after the location so the two kinds of message share a prefix and sort together.
     */
    public static String of(long row, String objectName, long readStartByte, String column) {
        return of(row, objectName, readStartByte) + ", column [" + column + "]";
    }
}
