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
 * ordinal, which a stream-compressed object reaches once it is read in more than one chunk, each chunk's
 * decoder counting from zero. Whether it is read that way is decided above this class, and not every such read
 * is: {@code AsyncExternalSourceOperatorFactory.openWithParallelism} returns no parallel source when a
 * {@code LIMIT} was pushed or {@code external_parsing_parallelism} is 1, and a single-chunk read counts from
 * the file's own start. {@code InformationalWarningBudget} deduplicates by exact string, so two chunks that
 * did collide previously collapsed into one warning with nothing to say a second record had failed.
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
     * this clause travels as a {@code Warning} response header, and {@code HeaderWarning}'s
     * {@code doesNotNeedEncoding} set leaves out every character above {@code U+00FF} (its BitSet is
     * {@code 1 + 0xFF} wide), so a single-character ellipsis is transmitted as {@code %E2%80%A6}.
     * It is only the Latin-1 range that rides along unencoded, which is why the marker is plain ASCII.
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
     * split it is not. Sharing the rendering also keeps the two kinds of message agreeing about the window: a
     * detail saying {@code counted from byte [B]} beside a summary quietly implying byte zero is a worse pair
     * than either message alone. Empty for a read from the start of the file, whose counts ARE the file's.
     * <p>
     * The byte names its own axis because on a stream-compressed object it is not an offset anyone can seek to in
     * the stored object: a gzip or zstd read is segmented and each segment's start is a cumulative offset into the
     * DECOMPRESSED stream, so printed bare beside the stored object's name it exceeds that object's length and a
     * user following it seeks past the end of the {@code .gz}. Said unconditionally because the axis claim is true
     * of a plain file too, whose decoded stream is its stored bytes, and the read context carries no compression
     * signal to branch on.
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
     * <p>
     * For a bare object name only. A whole location goes through {@link #escapedForDisplay} instead, which escapes
     * without capping, because the cap's accepted collision is the wrong trade there.
     */
    public static String displayObjectName(String objectName) {
        return objectName == null ? "" : shorten(objectName);
    }

    /**
     * The escape alone, for store-supplied text that is about to be printed inside brackets, and deliberately
     * no cap. A whole source location is the main caller; a column name is the other.
     * <p>
     * {@link #displayObjectName} accepts that two names agreeing on the kept head and tail reduce to one token,
     * because the alternative is an unbounded name in a response header and because the names it reduces are
     * already ambiguous: a folder of objects sharing a basename collides through {@link StoragePath#objectName()}
     * before any cap applies. A location is the string that RESOLVES that collision, so capping it destroys the
     * one distinction it was being printed for. The discriminating segment of a path sits wherever the layout
     * puts it -- a region, a date, a partition key -- which is to say inside the window a fixed-width cut
     * removes, and the warning text is itself the deduplication key on both sinks, so the collision is not a
     * shortened message but a deleted one. {@code NdJsonFormatReaderTests} pins a glob of two objects whose
     * locations differ only there.
     * <p>
     * What this does not buy is a bound on width, and that is the honest cost. A key may be any length a store
     * permits, and each escaped character costs six where the original cost one, so a location in a response
     * header is as long as the store made it and, for a key written entirely in the escaped set, six times that.
     * Nothing downstream re-bounds it: {@code http.max_warning_header_size} and {@code http.max_warning_header_count}
     * default to unbounded, and the informational channel caps the NUMBER of distinct warnings
     * ({@code SkipWarnings#MAX_ADDED_WARNINGS}) rather than the length of any one. The merge base embedded the
     * same location raw and uncapped, so what this adds to the width is the expansion, not the absence of a
     * bound. The escape is the half that closes a defect -- a name cannot forge the bracket structure around it
     * either way -- while the cap only ever traded width for ambiguity, which is a trade worth making for a name
     * and not for a location. Bounding a location without losing the distinction it carries means a discriminator
     * that survives the cut, which is a policy owed to every message embedding a store-supplied path rather than
     * to this one.
     * <p>
     * The cost, which is real and accepted: a key legitimately holding a bracket -- {@code part[0].ndjson} is an
     * ordinary name -- renders escaped, so what the warning prints is no longer a string the store will accept
     * back. A location whose authority is an IPv6 literal carries the same cost on brackets the URI grammar
     * contributed rather than the key, so {@code https://[2001:db8::1]/b/x} renders ten characters wider. Once a location is one string those brackets are the same characters a key
     * can spell, so telling them apart means trusting a parse of the very text this is protecting; the address
     * stays legible escaped, and an unescaped prefix would reopen the forgery for a container name.
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
        // Both cuts fall on an arbitrary UTF-16 index, so either can land between the halves of a surrogate
        // pair and leave an unpaired one. One step each way is enough for a name decoded from UTF-8, where a high
        // surrogate is always followed by its low one. Give the orphan up rather than keep it: a lone surrogate encodes to
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
     * Replaces — with a backslash-u escape of its code point, six ASCII characters where the original was one —
     * every character in an object name that the name could otherwise use to impersonate the text around it. The
     * escape is spelled out in words rather than written: a backslash followed by {@code u} is a unicode escape
     * ANYWHERE in a Java source file, comments included, so writing one here would have the compiler substitute
     * the character it names before anyone could read about it. Two such characters, for two different channels.
     * <p>
     * A control character, because this clause reaches the node log: the readers hand the whole rendered location
     * to the log beside the client warning, so the operator reading the log and the user reading the response see
     * the same row of the same object. They do not log it at the same level -- CSV at {@code warn}, NDJSON at
     * {@code info} or {@code debug} by the error policy -- which changes who sees it by default but not that a
     * name reaching a log layout can forge a line in it.
     * <p>
     * This clause is not the only path a store-supplied string takes to that log, and saying so would invite the
     * next log site to skip the escape. {@code NdJsonPageIterator}'s truncated-record branch logs a location at
     * {@code warn} and other sites log one at {@code debug}; the first of those now goes through
     * {@link #escapedForDisplay} for this reason, while the {@code debug} sites are left as they are. What this
     * class is responsible for is the clause, and any other site printing a store-supplied string owes itself
     * the same escape. A name is store-supplied
     * text — an S3 key may hold any UTF-8, newline included, and this class's parser keeps the last path segment
     * verbatim — and the console and legacy log layouts write a message as it stands, so an embedded newline
     * would let a key counterfeit a second log line. The response channel is already safe and stays so:
     * {@code HeaderWarning.encode} leaves the C0 range out of its unencoded set, so a newline travels as
     * {@code %0A} with or without this.
     * <p>
     * A square bracket, because the name is printed inside one and the clause is built by concatenation, so a name
     * is free to close its own bracket and open the next field: an object actually called
     * {@code v.csv] counted from byte [12} renders character-for-character what a byte-12 read of {@code v.csv}
     * renders. {@code InformationalWarningBudget} deduplicates by exact string, so the forged detail and the real
     * one collapse into one warning and the user is told about one failing row where there were two. Both brackets
     * are escaped, so the rule is "a bracket in a name is never a bracket in the message" rather than a rule about
     * which one happened to matter.
     * <p>
     * Escaping is one-way, which is the limit of what this buys: a name literally holding the six characters of a
     * bracket's escape renders exactly as one holding the bracket, so two names can still collapse to one token
     * and one of their warnings can still be deduplicated away. Escaping the backslash as well would close that,
     * and is declined, because this method's own output would then hold an escapable character while {@link #of}
     * applies it a second time over an already-reduced name — the idempotence {@code RowLocationTests} pins, so
     * adding to the escaped set fails a test rather than silently double-escaping every bracketed name.
     * <p>
     * Applied before the cap, so {@link #MAX_OBJECT_NAME_CHARS} bounds what is actually emitted: an escape is six
     * characters where the original was one, and capping first would let a name of them expand past the bound.
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
        return of(row, objectName, readStartByte) + ", column [" + escapedForDisplay(column) + "]";
    }
}
