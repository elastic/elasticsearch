/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * CSV record-boundary splitter for byte-oriented parallel parsing.
 */
final class CsvRecordSplitter implements RecordSplitter {

    private final CsvFormatOptions options;
    private final int maxRecordBytes;

    CsvRecordSplitter(CsvFormatOptions options, int maxRecordBytes) {
        this.options = Objects.requireNonNull(options);
        if (maxRecordBytes <= 0) {
            throw new IllegalArgumentException("maxRecordBytes must be positive, got: " + maxRecordBytes);
        }
        this.maxRecordBytes = maxRecordBytes;
    }

    @Override
    public long findNextRecordBoundary(InputStream stream) throws IOException {
        if (options.multiValueSyntax() != CsvFormatOptions.MultiValueSyntax.BRACKETS) {
            return findNextRecordBoundaryQuotedFieldsOnly(stream);
        }
        BufferedInputStream bis = stream instanceof BufferedInputStream b ? b : new BufferedInputStream(stream);
        int markLimit = recordBoundaryMarkLimit();
        long maxMvcSuffixBytes = Math.max(0L, markLimit - 1L);
        return findNextRecordBoundaryBracketCommaMvc(bis, markLimit, maxMvcSuffixBytes);
    }

    /**
     * Override the default for the QuotedFieldsOnly path so the streaming segmentator gets a
     * single-pass answer instead of dispatching the per-record scanner once per record. Bracket MVC
     * stays on the forward scanner because the bracket-region state machine (depth,
     * leading-whitespace gating, mark limit) is non-trivial to fold into a single pass.
     */
    @Override
    public int findLastRecordBoundary(byte[] buf, int offset, int length) throws IOException {
        if (length <= 0) {
            return -1;
        }
        Objects.checkFromIndexSize(offset, length, buf.length);
        if (options.multiValueSyntax() != CsvFormatOptions.MultiValueSyntax.BRACKETS) {
            return findLastRecordBoundaryQuotedFieldsOnly(buf, offset, length);
        }
        return findLastRecordBoundaryByForwardScan(buf, offset, length);
    }

    @Override
    public int maxRecordBytes() {
        return maxRecordBytes;
    }

    /**
     * A quoted field (or a bracketed multi-value cell) can carry a record across a raw newline, and a
     * backslash-escaped raw newline is likewise in-field content, so a probe that starts at an arbitrary
     * offset can land mid-construct and misread an embedded newline as a record terminator. This splitter
     * therefore handles every non-plain combination (quoting on, escaping on, or both) and must be driven
     * sequentially from a known record start. Only the plain no-quote/no-escape case (which
     * {@code CsvFormatReader.recordSplitter} routes to {@code NewlineRecordSplitter}) keeps the strided
     * default.
     */
    @Override
    public boolean supportsStridedProbing() {
        return false;
    }

    /**
     * The quoted-fields grammar (quoting and/or escaping, no bracket MVC) can prove a record start at an
     * arbitrary offset via {@link #findProvenRecordBoundary(InputStream)} plus the
     * {@link #findRecordStartAtOrAfter(InputStream, long, BooleanSupplier)} exact walk, restoring cross-node
     * macro splitting for quoted CSV/TSV. Bracket MVC is excluded: {@code bracketDepth} is unbounded, so the
     * entry states at a mid-file offset cannot be enumerated, and it stays whole-file.
     */
    @Override
    public boolean supportsProvenProbing() {
        return options.multiValueSyntax() != CsvFormatOptions.MultiValueSyntax.BRACKETS;
    }

    /**
     * Upper bound on how far a single {@link #findProvenRecordBoundary(InputStream)} pass reads before giving up.
     * A probe runs at every stride/segment boundary, so a probe that fails to converge must stay cheap; this caps
     * that worst case. It is a performance knob, not a correctness one: a probe that cannot converge within it
     * yields {@link #AMBIGUOUS} and the caller falls back to the exact walk.
     */
    private static final long PROBE_CONVERGENCE_WINDOW_BYTES = 1024L * 1024L; // 1 MiB

    /**
     * Hard cap on a single {@link #findProvenRecordBoundary(InputStream)} pass (advance to a clean symbol plus the
     * lockstep scan). Capped at {@link #PROBE_CONVERGENCE_WINDOW_BYTES} so a failed probe on a quote-free window reads
     * little before falling back, and also bounded by {@code maxRecordBytes} so a probe can never scan a whole
     * oversized record's body before giving up. This is a performance knob, not a correctness one: a probe that
     * cannot converge within it yields {@link #AMBIGUOUS} and defers to the exact walk, which owns the
     * {@link #RECORD_TOO_LARGE} verdict. It does not mean the proved boundary always terminates a record shorter than
     * {@code maxRecordBytes}: a record that began before the probe offset can be over the cap yet close within the
     * window, so the probe converges at its terminator. That boundary is still a true record start, and the oversized
     * record still fails loudly wherever it is actually read.
     */
    private long convergenceWindow() {
        return Math.min((long) maxRecordBytes, PROBE_CONVERGENCE_WINDOW_BYTES);
    }

    /** Poll the cancellation supplier this often (in bytes) during the exact walk. */
    private static final long CANCEL_CHECK_INTERVAL_BYTES = 64 * 1024;

    /**
     * Whether {@code b} (an unsigned byte value {@code 0..255}) is a "clean symbol": non-whitespace plain content
     * under the enabled options, i.e. not a structural byte ({@code "} when quoting, the escape char when escaping,
     * the delimiter, {@code \r}, {@code \n}) and not ASCII field-leading whitespace. Reading such a byte lands both
     * hypotheses in {@code (·, content)} regardless of the entry state, collapsing the five feasible entry states
     * to two. Over-including a byte only forces more {@link #AMBIGUOUS} fallbacks, never a wrong boundary.
     */
    private boolean isCleanSymbol(int b, boolean quoteAware, boolean escapeAware, int quoteChar, int escapeChar, int delimiter) {
        if (b == '\r' || b == '\n' || b == delimiter) {
            return false;
        }
        if (quoteAware && b == quoteChar) {
            return false;
        }
        if (escapeAware && b == escapeChar) {
            return false;
        }
        return CsvFormatReader.isAsciiCsvFieldLeadingWhitespace(b) == false;
    }

    /**
     * Per-hypothesis parse state for the lockstep probe. The one-byte lookaheads the peek-based scanners do via
     * stream reads (doubled {@code ""}, CRLF) are re-expressed here as pending flags so the single shared cursor
     * advances exactly one byte per step while two hypotheses that interpret the same byte differently stay synced.
     * At most one pending flag is set at a time (each is set by a distinct byte and cleared on the next).
     */
    private static final class ProbeState {
        boolean inQuotes;
        boolean fieldHasNonWhitespace;
        /** Previous byte was the escape char; this byte is escaped content (consumed verbatim). */
        boolean pendingEscape;
        /** Inside quotes, previous byte was a quote char; this byte decides doubled-literal vs field close. */
        boolean pendingQuote;
        /** Outside quotes, previous byte was {@code \r}; this byte decides CRLF vs lone-CR terminator. */
        boolean pendingCr;
        /** Offset (bytes consumed) through the most recently completed record terminator. */
        long boundaryOffset;
    }

    /**
     * Feeds one byte to one hypothesis, mirroring {@link CsvLogicalRecordReader#readRecord}'s byte ordering
     * (escape before quote; quote open only at field start). Returns the offset (bytes consumed through the
     * terminator) if a record terminator <em>finalized</em> during this step, otherwise {@code -1}. A {@code \r}
     * defers finalization to the next byte (CRLF vs lone-CR); when a pending lookahead resolves, the current byte
     * is interpreted in the resolved state within the same step.
     *
     * @param consumed number of bytes consumed including {@code b}
     */
    private long stepProbe(
        ProbeState s,
        int b,
        long consumed,
        boolean quoteAware,
        boolean escapeAware,
        int quoteChar,
        int escapeChar,
        int delimiter
    ) {
        if (s.pendingCr) {
            s.pendingCr = false;
            if (b == '\n') {
                // CRLF: extend the terminator to include this \n. State was reset to record-start at the \r.
                s.boundaryOffset = consumed;
                return consumed;
            }
            // Lone CR: the terminator finalized at the \r; reprocess b as the first byte of the new record.
            long loneCr = s.boundaryOffset;
            processOutOfQuote(s, b, consumed, quoteAware, escapeAware, quoteChar, escapeChar, delimiter);
            return loneCr;
        }
        if (s.pendingQuote) {
            s.pendingQuote = false;
            if (b == quoteChar) {
                // Doubled "" literal inside the quoted field: stay in quotes.
                return -1;
            }
            // Lone quote closed the field; interpret b in the resolved out-of-quote state.
            s.inQuotes = false;
            return processOutOfQuote(s, b, consumed, quoteAware, escapeAware, quoteChar, escapeChar, delimiter);
        }
        if (s.pendingEscape) {
            s.pendingEscape = false;
            if (s.inQuotes == false) {
                s.fieldHasNonWhitespace = true;
            }
            return -1;
        }
        if (escapeAware && b == escapeChar) {
            s.pendingEscape = true;
            return -1;
        }
        if (s.inQuotes) {
            if (b == quoteChar) {
                s.pendingQuote = true;
            }
            return -1;
        }
        return processOutOfQuote(s, b, consumed, quoteAware, escapeAware, quoteChar, escapeChar, delimiter);
    }

    /**
     * Processes {@code b} for a hypothesis known to be out of quotes (fresh record-start or mid-field). Returns the
     * finalized terminator offset for a lone {@code \n}, {@code -1} otherwise (a {@code \r} defers via
     * {@link ProbeState#pendingCr}).
     */
    private long processOutOfQuote(
        ProbeState s,
        int b,
        long consumed,
        boolean quoteAware,
        boolean escapeAware,
        int quoteChar,
        int escapeChar,
        int delimiter
    ) {
        if (escapeAware && b == escapeChar) {
            s.pendingEscape = true;
            return -1;
        }
        if (b == '\n') {
            s.boundaryOffset = consumed;
            s.fieldHasNonWhitespace = false;
            return consumed;
        }
        if (b == '\r') {
            // Terminator finalizes here at the earliest (lone CR); may extend to CRLF on the next byte.
            s.boundaryOffset = consumed;
            s.fieldHasNonWhitespace = false;
            s.pendingCr = true;
            return -1;
        }
        if (b == delimiter) {
            s.fieldHasNonWhitespace = false;
            return -1;
        }
        if (quoteAware && b == quoteChar && s.fieldHasNonWhitespace == false) {
            s.inQuotes = true;
            return -1;
        }
        if (CsvFormatReader.isAsciiCsvFieldLeadingWhitespace(b) == false) {
            s.fieldHasNonWhitespace = true;
        }
        return -1;
    }

    /**
     * Bounded two-hypothesis probe for a proven record start (see the class-level contract on
     * {@link RecordSplitter#findProvenRecordBoundary(InputStream)}). Advances grammar-blind to a clean symbol
     * {@code q}, then runs an out-of-quote hypothesis and (when quoting is on) an in-quote hypothesis forward in
     * lockstep, emitting a boundary only at the first record terminator where both are simultaneously out of quote.
     * When quoting is off the single deterministic hypothesis proves the boundary at its first terminator. Bounded
     * by {@link #convergenceWindow()}; returns {@link #AMBIGUOUS} on window exhaustion, EOF, or no clean symbol.
     */
    @Override
    public long findProvenRecordBoundary(InputStream stream) throws IOException {
        if (supportsProvenProbing() == false) {
            throw new UnsupportedOperationException("bracket multi-value CSV does not support proven probing");
        }
        BufferedInputStream bis = stream instanceof BufferedInputStream b ? b : new BufferedInputStream(stream);
        boolean quoteAware = options.quoting();
        boolean escapeAware = options.escaping();
        int quoteChar = options.quoteChar();
        int escapeChar = options.escapeChar();
        int delimiter = options.delimiter();
        long window = convergenceWindow();
        long consumed = 0;

        // Phase 1: advance grammar-blind to a clean symbol q. q's interpretation is entry-state-independent, so
        // mis-seeding (skipping an embedded terminator, quote, or escape pair) is harmless: nothing is emitted
        // until a proven convergence.
        boolean foundClean = false;
        while (consumed < window) {
            int ib = bis.read();
            if (ib == -1) {
                return AMBIGUOUS;
            }
            consumed++;
            if (isCleanSymbol(ib, quoteAware, escapeAware, quoteChar, escapeChar, delimiter)) {
                foundClean = true;
                break;
            }
        }
        if (foundClean == false) {
            return AMBIGUOUS;
        }

        // Phase 2: q is consumed; both hypotheses are now (·, content).
        ProbeState hOut = new ProbeState();
        hOut.fieldHasNonWhitespace = true;
        ProbeState hIn = null;
        if (quoteAware) {
            hIn = new ProbeState();
            hIn.inQuotes = true;
        }

        // Phase 3: lockstep scan.
        while (consumed < window) {
            int ib = bis.read();
            if (ib == -1) {
                return AMBIGUOUS;
            }
            consumed++;
            long bOut = stepProbe(hOut, ib, consumed, quoteAware, escapeAware, quoteChar, escapeChar, delimiter);
            if (quoteAware == false) {
                if (bOut >= 0) {
                    return bOut;
                }
                continue;
            }
            long bIn = stepProbe(hIn, ib, consumed, quoteAware, escapeAware, quoteChar, escapeChar, delimiter);
            if (bOut >= 0 && bOut == bIn) {
                // Both hypotheses out of quotes at the same terminator: proven record start whichever is true.
                return bOut;
            }
        }
        return AMBIGUOUS;
    }

    /**
     * Exact forward walk from a stream opened at a known record start (see the contract on
     * {@link RecordSplitter#findRecordStartAtOrAfter(InputStream, long, BooleanSupplier)}). Single quote-aware pass
     * keeping {@code inQuotes} across records, returning the first record-start offset {@code >= minSkip}.
     */
    @Override
    public long findRecordStartAtOrAfter(InputStream stream, long minSkip, BooleanSupplier isCancelled) throws IOException {
        if (supportsProvenProbing() == false) {
            throw new UnsupportedOperationException("bracket multi-value CSV does not support proven probing");
        }
        assert minSkip > 0 : "minSkip must be positive so the opening record start at offset 0 is never returned, got " + minSkip;
        BufferedInputStream bis = stream instanceof BufferedInputStream b ? b : new BufferedInputStream(stream);
        boolean quoteAware = options.quoting();
        boolean escapeAware = options.escaping();
        byte quoteAsByte = (byte) options.quoteChar();
        byte escAsByte = (byte) options.escapeChar();
        byte delimAsByte = (byte) options.delimiter();
        long consumed = 0;
        long recordStart = 0;
        boolean inQuotes = false;
        boolean fieldHasNonWhitespace = false;
        long sinceCancelCheck = 0;

        while (true) {
            int ib = bis.read();
            if (ib == -1) {
                return -1;
            }
            consumed++;
            if (++sinceCancelCheck >= CANCEL_CHECK_INTERVAL_BYTES) {
                sinceCancelCheck = 0;
                if (isCancelled.getAsBoolean()) {
                    throw new TaskCancelledException("split discovery cancelled");
                }
            }
            if (consumed - recordStart > maxRecordBytes) {
                return RECORD_TOO_LARGE;
            }
            byte b = (byte) ib;
            if (escapeAware && b == escAsByte) {
                int esc = bis.read();
                if (esc != -1) {
                    consumed++;
                    if (consumed - recordStart > maxRecordBytes) {
                        return RECORD_TOO_LARGE;
                    }
                }
                if (inQuotes == false) {
                    fieldHasNonWhitespace = true;
                }
                continue;
            }
            if (inQuotes) {
                if (b == quoteAsByte) {
                    if ((byte) peekByte(bis) == quoteAsByte) {
                        bis.read();
                        consumed++;
                        if (consumed - recordStart > maxRecordBytes) {
                            return RECORD_TOO_LARGE;
                        }
                        continue;
                    }
                    inQuotes = false;
                }
                continue;
            }
            if (b == '\n') {
                recordStart = consumed;
                fieldHasNonWhitespace = false;
                if (recordStart >= minSkip) {
                    return recordStart;
                }
                continue;
            }
            if (b == '\r') {
                bis.mark(1);
                int next = bis.read();
                if (next == '\n') {
                    consumed++;
                    if (consumed - recordStart > maxRecordBytes) {
                        return RECORD_TOO_LARGE;
                    }
                } else if (next != -1) {
                    bis.reset();
                }
                recordStart = consumed;
                fieldHasNonWhitespace = false;
                if (recordStart >= minSkip) {
                    return recordStart;
                }
                continue;
            }
            if (b == delimAsByte) {
                fieldHasNonWhitespace = false;
            } else if (quoteAware && b == quoteAsByte && fieldHasNonWhitespace == false) {
                inQuotes = true;
            } else if (CsvFormatReader.isAsciiCsvFieldLeadingWhitespace(ib & 0xff) == false) {
                fieldHasNonWhitespace = true;
            }
        }
    }

    private int findLastRecordBoundaryByForwardScan(byte[] buf, int offset, int length) throws IOException {
        if (length <= 0) {
            return -1;
        }
        int lastBoundary = -1;
        int cumulative = 0;
        while (cumulative < length) {
            long consumed = findNextRecordBoundary(new ByteArrayInputStream(buf, offset + cumulative, length - cumulative));
            if (consumed == RECORD_TOO_LARGE) {
                return lastBoundary >= 0 ? lastBoundary : (int) RECORD_TOO_LARGE;
            }
            if (consumed < 0) {
                return lastBoundary;
            }
            cumulative += Math.toIntExact(consumed);
            lastBoundary = offset + cumulative - 1;
        }
        return lastBoundary;
    }

    /**
     * Quoting and escaping rules mirror the tokenizer {@link CsvLogicalRecordReader#readRecord}: when quoting is on a
     * {@code quoteChar} opens a quoted field only at field start (after an unquoted {@code delimiter} or {@code \n},
     * optionally past field-leading whitespace), and a mid-field {@code quoteChar} is a literal that does not toggle
     * quote state; when escaping is on, an {@code escapeChar} (inside or outside a quoted field) carries the byte that
     * follows it into the field verbatim, so a backslash-escaped raw terminator is never a record boundary. When both
     * knobs are off this method is not used - {@link CsvFormatReader#recordSplitter} routes plain data to the strided
     * {@code NewlineRecordSplitter} instead.
     * <p>
     * Best-effort/open-tail contract: the scan assumes the buffer begins at a record boundary and advances
     * {@code lastBoundary} only on a true unquoted, unescaped record terminator. So a chunk the segmentator cut
     * mid-record yields no boundary inside that leading partial, and a genuinely unterminated quoted field keeps
     * {@code inQuotes == true} so its trailing {@code \n}s are skipped - the rule the grow loop requires.
     */
    private int findLastRecordBoundaryQuotedFieldsOnly(byte[] buf, int offset, int length) {
        if (length <= 0) {
            return -1;
        }
        int end = offset + length;
        int lastBoundary = -1;
        int recordStart = offset;
        boolean inQuotes = false;
        boolean fieldHasNonWhitespace = false;
        boolean quoteAware = options.quoting();
        boolean escapeAware = options.escaping();
        byte quoteAsByte = (byte) options.quoteChar();
        byte escAsByte = (byte) options.escapeChar();
        byte delimAsByte = (byte) options.delimiter();
        for (int i = offset; i < end; i++) {
            byte b = buf[i];
            if (escapeAware && b == escAsByte) {
                // The escape and the byte it escapes (a raw terminator, a quote, or the escape itself) are
                // in-field content, so skip the escaped byte; it can never be a record boundary. A lone
                // trailing escape at the buffer end simply consumes itself.
                i++;
                if (inQuotes == false) {
                    fieldHasNonWhitespace = true;
                }
                continue;
            }
            if (inQuotes) {
                if (b == quoteAsByte) {
                    if (i + 1 < end && buf[i + 1] == quoteAsByte) {
                        // Doubled quote inside a quoted field - RFC 4180 literal, stay in quotes.
                        i++;
                    } else {
                        inQuotes = false;
                    }
                }
                continue;
            }
            if (b == '\n') {
                if (recordExceedsLimit(recordStart, i)) {
                    return lastBoundary >= 0 ? lastBoundary : (int) RECORD_TOO_LARGE;
                }
                lastBoundary = i;
                recordStart = i + 1;
                fieldHasNonWhitespace = false;
            } else if (b == '\r') {
                int boundary = i;
                if (i + 1 < end && buf[i + 1] == '\n') {
                    boundary = ++i;
                }
                if (recordExceedsLimit(recordStart, boundary)) {
                    return lastBoundary >= 0 ? lastBoundary : (int) RECORD_TOO_LARGE;
                }
                lastBoundary = boundary;
                recordStart = boundary + 1;
                fieldHasNonWhitespace = false;
            } else if (b == delimAsByte) {
                fieldHasNonWhitespace = false;
            } else if (quoteAware && b == quoteAsByte && fieldHasNonWhitespace == false) {
                inQuotes = true;
            } else if (CsvFormatReader.isAsciiCsvFieldLeadingWhitespace(b & 0xff) == false) {
                fieldHasNonWhitespace = true;
            }
        }
        return end - recordStart > maxRecordBytes && lastBoundary < 0 ? (int) RECORD_TOO_LARGE : lastBoundary;
    }

    private boolean recordExceedsLimit(int recordStart, int boundary) {
        return boundary - recordStart + 1 > maxRecordBytes;
    }

    /**
     * Upper bound for {@link BufferedInputStream#mark(int)} while probing bracket MVC cells during record-boundary
     * scans. Matches {@link CsvFormatOptions#maxFieldSize()} so an unclosed bracket cell cannot invalidate the mark
     * before we reset and treat {@code [} as a literal byte.
     */
    private int recordBoundaryMarkLimit() {
        int maxField = options.maxFieldSize();
        if (maxField <= 0) {
            return Math.min(64 * 1024 * 1024, Integer.MAX_VALUE - 8);
        }
        return Math.min(maxField + 1024, Integer.MAX_VALUE - 8);
    }

    /**
     * Bytes consumed after an opening {@code [} until bracket depth returns to zero, or {@code -1} if EOF was reached
     * first or the scan exceeded {@link CsvFormatOptions#maxFieldSize()} (unclosed cell).
     */
    private long consumeBracketMvcSuffixBytes(BufferedInputStream in, long maxSuffixBytes) throws IOException {
        int depth = 1;
        long bytes = 0;
        while (depth > 0) {
            if (bytes >= maxSuffixBytes) {
                return -1;
            }
            int ib = in.read();
            if (ib == -1) {
                return -1;
            }
            bytes++;
            byte b = (byte) ib;
            if (b == '[') {
                depth++;
            } else if (b == ']') {
                depth--;
            }
        }
        return bytes;
    }

    /**
     * Record boundary scan for delimited text (CSV or TSV) with bracket MVC. Newlines inside {@code [..]} or quoted fields
     * must not end the record. Quote opening follows RFC 4180 - only at field start, optionally preceded by whitespace
     * - so stray {@code "} chars in unquoted cells do not trigger multi-line gluing or pathological segment splits.
     */
    private long findNextRecordBoundaryBracketCommaMvc(BufferedInputStream bis, int markLimit, long maxMvcSuffixBytes) throws IOException {
        long consumed = 0;
        boolean inQuotes = false;
        boolean fieldHasNonWhitespace = false;
        byte quoteAsByte = (byte) options.quoteChar();
        byte escAsByte = (byte) options.escapeChar();
        byte delimAsByte = (byte) options.delimiter();

        while (true) {
            int ib = bis.read();
            if (ib == -1) {
                return -1;
            }
            consumed++;
            if (consumed > maxRecordBytes) {
                return RECORD_TOO_LARGE;
            }
            byte b = (byte) ib;

            if (inQuotes) {
                if (b == quoteAsByte) {
                    bis.mark(2);
                    int ib2 = bis.read();
                    if (ib2 == -1) {
                        inQuotes = false;
                        continue;
                    }
                    if ((byte) ib2 == quoteAsByte) {
                        consumed++;
                        if (consumed > maxRecordBytes) {
                            return RECORD_TOO_LARGE;
                        }
                        continue;
                    }
                    bis.reset();
                    inQuotes = false;
                } else if (b == escAsByte) {
                    bis.mark(2);
                    int ib2 = bis.read();
                    if (ib2 != -1 && (byte) ib2 == delimAsByte) {
                        consumed++;
                        if (consumed > maxRecordBytes) {
                            return RECORD_TOO_LARGE;
                        }
                        continue;
                    }
                    bis.reset();
                }
                continue;
            }

            if (b == '\n') {
                return consumed;
            }
            if (b == '\r') {
                return consumeCrTerminator(bis, consumed);
            }
            if (b == delimAsByte) {
                fieldHasNonWhitespace = false;
                continue;
            }
            if (b == quoteAsByte && fieldHasNonWhitespace == false) {
                inQuotes = true;
                continue;
            }
            if (b == '[' && fieldHasNonWhitespace == false) {
                bis.mark(markLimit);
                long suffix = consumeBracketMvcSuffixBytes(bis, maxMvcSuffixBytes);
                if (suffix >= 0) {
                    consumed += suffix;
                    if (consumed > maxRecordBytes) {
                        return RECORD_TOO_LARGE;
                    }
                    fieldHasNonWhitespace = true;
                    continue;
                }
                bis.reset();
                fieldHasNonWhitespace = true;
                continue;
            }
            if (CsvFormatReader.isAsciiCsvFieldLeadingWhitespace(ib & 0xff) == false) {
                fieldHasNonWhitespace = true;
            }
        }
    }

    /**
     * Returns the next byte without consuming it (or {@code -1} at EOF). Encapsulates the single-byte
     * {@code mark}/{@code read}/{@code reset} so callers cannot invalidate the mark with a second read.
     */
    private static int peekByte(BufferedInputStream bis) throws IOException {
        bis.mark(1);
        int b = bis.read();
        bis.reset();
        return b;
    }

    /**
     * Per-byte scan over a {@link BufferedInputStream} - no per-call bulk read buffer is allocated;
     * an existing {@link BufferedInputStream} input is reused, otherwise the stream is wrapped once.
     * Applies the same field-start quoting and escaping rules as the actual tokenizer
     * {@link CsvLogicalRecordReader#readRecord} and as {@link #findNextRecordBoundaryBracketCommaMvc}: when quoting is
     * on a {@code quoteChar} opens a quoted field only at field start (optionally after field-leading whitespace) and
     * a mid-field {@code quoteChar} is a literal; when escaping is on an {@code escapeChar} carries the byte that
     * follows it into the field verbatim (inside or outside a quoted field), so an escaped raw terminator does not end
     * the record. Returns the byte count up to and including the first unquoted, unescaped record-terminating
     * {@code \n}, or {@code -1} at EOF.
     */
    private long findNextRecordBoundaryQuotedFieldsOnly(InputStream stream) throws IOException {
        BufferedInputStream bis = stream instanceof BufferedInputStream b ? b : new BufferedInputStream(stream);
        long consumed = 0;
        boolean inQuotes = false;
        boolean fieldHasNonWhitespace = false;
        boolean quoteAware = options.quoting();
        boolean escapeAware = options.escaping();
        byte quoteAsByte = (byte) options.quoteChar();
        byte escAsByte = (byte) options.escapeChar();
        byte delimAsByte = (byte) options.delimiter();
        while (true) {
            int ib = bis.read();
            if (ib == -1) {
                return -1;
            }
            consumed++;
            if (consumed > maxRecordBytes) {
                return RECORD_TOO_LARGE;
            }
            byte b = (byte) ib;
            if (escapeAware && b == escAsByte) {
                // Consume the escaped byte verbatim (a raw terminator or quote here is in-field content). A
                // lone escape at EOF just consumes itself.
                int esc = bis.read();
                if (esc != -1) {
                    consumed++;
                    if (consumed > maxRecordBytes) {
                        return RECORD_TOO_LARGE;
                    }
                }
                if (inQuotes == false) {
                    fieldHasNonWhitespace = true;
                }
                continue;
            }
            if (inQuotes) {
                if (b == quoteAsByte) {
                    // A doubled "" is a literal (stay in quotes); a lone " closes the field. peekByte
                    // leaves the non-doubled byte in the stream to be re-read by the next iteration.
                    if ((byte) peekByte(bis) == quoteAsByte) {
                        bis.read(); // consume the second quote of the doubled pair
                        consumed++;
                        if (consumed > maxRecordBytes) {
                            return RECORD_TOO_LARGE;
                        }
                        continue;
                    }
                    inQuotes = false;
                }
                continue;
            }
            if (b == '\n') {
                return consumed;
            }
            if (b == '\r') {
                return consumeCrTerminator(bis, consumed);
            }
            if (b == delimAsByte) {
                fieldHasNonWhitespace = false;
            } else if (quoteAware && b == quoteAsByte && fieldHasNonWhitespace == false) {
                inQuotes = true;
            } else if (CsvFormatReader.isAsciiCsvFieldLeadingWhitespace(ib & 0xff) == false) {
                fieldHasNonWhitespace = true;
            }
        }
    }

    private long consumeCrTerminator(BufferedInputStream bis, long consumed) throws IOException {
        bis.mark(1);
        int next = bis.read();
        if (next == '\n') {
            long consumedWithLf = consumed + 1;
            return consumedWithLf > maxRecordBytes ? RECORD_TOO_LARGE : consumedWithLf;
        }
        if (next != -1) {
            bis.reset();
        }
        return consumed;
    }
}
