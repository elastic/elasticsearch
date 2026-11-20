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
 * under a single summary warning. It does not fully disambiguate them: {@link StoragePath#objectName()}
 * is the last path segment by design — it never returns the full URI, failing closed rather than leaking a
 * bucket or prefix — so a glob over objects sharing a basename names every one of them the same, and that
 * collision survives this class. What the byte clause does fix is two chunks of ONE object failing at the same
 * ordinal, which is every gzip read, since the stream is segmented and each segment's decoder counts from
 * zero; {@code InformationalWarningBudget} deduplicates by exact string, so those previously collapsed into
 * one warning with nothing to say a second record had failed.
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
        return new StringBuilder("row [").append(row)
            .append(']')
            .append(objectClause(" of", displayObjectName(objectName)))
            .append(countedIn(readStartByte))
            .toString();
    }

    /**
     * The bracketed object part of a message, with {@code leadIn} carrying the preposition ({@code " of"},
     * {@code " for"}, or empty where the sentence already supplies the word) so the whole phrase disappears
     * together when the name is unavailable, rather than leaving a dangling preposition or an empty
     * {@code []}.
     * <p>
     * One owner because every message that names an object has to agree about that, and they did not: the
     * schema-path refusals, the per-row clause {@link #of} builds and the sample-budget refusals each rendered
     * their own copy of it, which agree only until one of them is edited. The name is expected to have been
     * reduced by {@link #displayObjectName} already, so this neither caps nor escapes.
     */
    public static String objectClause(String leadIn, String objectName) {
        return objectName == null || objectName.isEmpty() ? "" : leadIn + " [" + objectName + "]";
    }

    /**
     * The window clause on its own: what {@link #of} appends after the row and the object, for a message that
     * reports an AGGREGATE rather than one row and so has no ordinal to carry.
     * <p>
     * Both readers' error-budget failures say how many rows they read before giving up, and that count is
     * split-relative for the same reason a row ordinal is — the reader counts its own input. Without this clause
     * {@code [N] errors in [M] rows of [object]} reads as a statement about the whole object, which on a later
     * split it is not, so the aggregate reproduced exactly the mis-location this class removes from the per-row
     * text. Sharing the rendering keeps the two kinds of message agreeing about the window: a detail line that
     * says {@code counted from byte [B]} and a summary that quietly implied byte zero would be a worse pair than
     * either message alone.
     * <p>
     * Returns the empty string for a read from the start of the file, whose counts ARE the file's and need no
     * qualification.
     * <p>
     * The byte names its own axis because on a stream-compressed object it is not an offset anyone can seek to
     * in the stored object. A gzip or zstd read is segmented and each segment's start is a cumulative offset
     * into the DECOMPRESSED stream, which is the axis the records sit on and the only one the reader is given;
     * printed bare beside the stored object's name it exceeds that object's length, so a user following it
     * seeks past the end of the {@code .gz}. Naming the axis is accurate for a plain file too, whose decoded
     * stream is its stored bytes, which is why it is said unconditionally rather than only where the reader
     * suspects compression -- it is not told. Placing the byte in the stored object for a compressed read needs
     * a compression signal the read context does not carry.
     */
    public static String countedIn(long readStartByte) {
        if (readStartByte > 0L) {
            return " counted from byte [" + readStartByte + "] of the decoded stream";
        }
        return readStartByte < 0L ? " counted within a split" : "";
    }

    /**
     * Reduces an object name to the token every message must use for it. A reader applies this where it assigns
     * its message-location field, so the summary warning and the per-row details sitting under it carry the SAME
     * token: capping only inside {@link #of} would make a long name render two ways in one response, which reads
     * as two different objects. It also escapes the characters a name could use to impersonate the text around
     * it, for the reasons given on {@link #escapeImpersonatingCharacters}. {@link #of} applies both again,
     * idempotently — escaped text holds no such character left to escape, and a name already inside the cap is
     * returned as it stands — so a caller that has not cannot emit an uncut or unescaped name. This is the one
     * statement of that rationale; call sites point here rather than repeat it.
     * <p>
     * Two names agreeing on the kept head and tail reduce to one token and the exact-string deduplication then
     * keeps only the first — the shared-basename collision above, reached through the cap instead of the path,
     * and accepted for the same reason: the alternative is an unbounded name in a response header. That covers
     * names that coincide by accident. A name BUILT to impersonate the text around it is a different thing and
     * is closed off separately, by {@link #escapeImpersonatingCharacters}.
     * <p>
     * Cuts to {@link #MAX_OBJECT_NAME_CHARS}, keeping both ends. Same head/tail shape as
     * {@link ErrorExcerpts#summarize}, without its character count: the count tells a user how much of a VALUE
     * they cannot see, which is actionable, whereas the length of a key they chose is not.
     */
    public static String displayObjectName(String objectName) {
        return objectName == null ? "" : shorten(objectName);
    }

    private static String shorten(String objectName) {
        String displayable = escapeImpersonatingCharacters(objectName);
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
     * Replaces, with a backslash-u escape of its code point — six ASCII characters where the original was one,
     * the last four naming the code point in hex, so a newline is escaped as 000A — every character in an
     * object name that the name could otherwise use to impersonate the text around it. Two such characters, for
     * two different channels. The escape is spelled out in words here rather than written: a backslash followed
     * by {@code u} is a unicode escape ANYWHERE in a Java source file, comments included, so writing one would
     * have the compiler substitute the character it names before anyone could read about it.
     * <p>
     * A control character, because this clause is
     * the one piece of an object's name that reaches the node log: the readers pass the whole rendered location
     * to {@code logger.warn} alongside the client warning, so that the operator reading the log and the user
     * reading the response see the same row of the same object. A name is store-supplied text — an S3 key may
     * hold any UTF-8, newline included, and this class's parser keeps the last path segment verbatim — and the
     * console and legacy log layouts write a message as it stands, so an embedded newline would let a key
     * counterfeit a second log line. The response channel is already safe and stays so:
     * {@code HeaderWarning.encode} leaves the C0 range out of its unencoded set, so a newline travels as
     * {@code %0A} with or without this.
     * <p>
     * A square bracket, because the name is printed inside one and the clause is built by concatenation, so a name
     * is free to close its own bracket and open the next field: an object actually called
     * {@code v.csv] counted from byte [12} renders {@code row [1] of [v.csv] counted from byte [12]}, which is
     * character-for-character what a byte-12 read of {@code v.csv} renders. That is worse than a merely confusing
     * name, because {@code InformationalWarningBudget} deduplicates by exact string: the forged detail and the
     * real one collapse into one warning and the user is told about one failing row where there were two. Escaping
     * either bracket is enough to break the forgery; both are escaped so the rule is "a bracket in a name is never
     * a bracket in the message" rather than a rule about which one happened to matter.
     * <p>
     * Escaping is one-way: a name literally holding the six characters of a bracket's escape renders exactly
     * as one holding the bracket, so two names can still reach one token. Escaping the backslash as well would
     * close that, and is declined, because the escape's own output would then hold an escapable character —
     * and {@link #of} applies this a second time over an already-reduced name, the idempotence that stops a
     * reader which forgot to reduce its own name from emitting an unescaped one. {@code RowLocationTests} pins
     * that, so adding to the escaped set fails a test rather than silently double-escaping every bracketed name.
     * <p>
     * Applied before the cap, so {@link #MAX_OBJECT_NAME_CHARS} bounds what is actually emitted — an escape is
     * six characters where the original was one, and capping first would let a name of them expand past the
     * bound afterwards.
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
     * {@link #of} with the column clause the per-cell messages carry: {@code row [N] of [object], column [c]}.
     * The column goes after the location so the two kinds of message share a prefix and sort together.
     */
    public static String of(long row, String objectName, long readStartByte, String column) {
        return of(row, objectName, readStartByte) + ", column [" + column + "]";
    }
}
