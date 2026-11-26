/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.xpack.esql.datasources.spi.ExternalFailures;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Reads one logical CSV record at a time while enforcing the query's byte-sized record cap.
 */
final class CsvLogicalRecordReader {

    private final Reader reader;
    private final char quoteChar;
    private final char delimiter;
    private final char escapeChar;
    private final int maxRecordBytes;
    private final Charset charset;
    private final boolean utf8;
    /**
     * Encoded-byte length of the most recently returned record, including its line terminator(s).
     * Set just before each {@link #readRecord(boolean)} return so callers can derive a record's
     * start offset as {@code bytesRead() - lastRecordBytes()} without re-walking the input.
     */
    private int lastRecordBytes = 0;
    /** Whether the record {@link #readRecord(boolean)} last returned ended with a line terminator. */
    private boolean lastRecordTerminated = true;
    /**
     * Cumulative encoded-byte count of every record this reader has returned (including terminators
     * and partial records consumed by the caller — schema discovery, header skipping, mid-record
     * resync). Anchors the file-global byte offset CSV emits on the {@code _rowPosition} channel,
     * mirroring the NDJSON {@code recordFileOffset} computation in
     * {@code NdJsonPageDecoder#recordFileOffset()}.
     */
    private long bytesRead = 0L;
    /**
     * Whether {@link #quoteChar} opens a quoted field at field start. {@code false} for the no-quote
     * modes ({@code plain}/{@code escaped}), where a quote byte is ordinary data and a raw line
     * terminator always ends the record. This keeps no-quote records from ever gluing across lines
     * on a stray quote.
     */
    private final boolean quoteAware;
    /**
     * Whether {@link #escapeChar} is consulted so an escaped character is carried into the record
     * verbatim (the escape and the char it escapes) rather than being interpreted structurally. This
     * matters for the direct-to-block quoted path (RFC&nbsp;4180 + backslash escapes), where Jackson
     * treats {@code \}+terminator and {@code \}+quote as in-field content: without this flag an
     * escaped newline would split the record and an escaped quote would toggle quote state, both of
     * which would diverge from Jackson. {@code false} for every other caller, leaving their behavior
     * untouched.
     */
    private final boolean escapeAware;
    /** Whether a record that spans a line terminator fails the read; see {@link #failOnLineSpan()}. */
    private boolean failOnLineSpan;

    /**
     * Bulk input buffer, used only in {@link #bulkBuffered} mode. The record state machine pulls
     * characters via {@link #readChar()}; when bulk buffering is on it refills with a single
     * {@link Reader#read(char[], int, int)} per chunk instead of one (synchronized) {@link Reader#read()}
     * per character, and {@link #pending} holds the single-character lookahead for doubled-quote and
     * CR/LF detection.
     *
     * <p>Bulk buffering reads ahead, so it may only be enabled when this reader owns the stream for the
     * remainder of the read (the direct-to-block path, or the house per-record path). The Jackson path
     * skips the header through this reader and then resumes tokenizing from the same underlying
     * {@link Reader}, so it must keep the non-buffered mode: exactly one underlying {@link Reader#read()}
     * per character, with lookahead un-read via {@code mark}/{@code reset} so no byte is swallowed
     * before Jackson resumes.
     */
    private static final int INPUT_BUFFER_SIZE = 8192;
    private static final int NO_PENDING = Integer.MIN_VALUE;
    private boolean bulkBuffered;
    private char[] inBuf;
    private int inPos;
    private int inLimit;
    private boolean eof;
    private int pending = NO_PENDING;

    /**
     * Reusable record buffer for the bulk-mode {@link #nextRecord()}. Each logical record is copied here
     * (terminators stripped, quotes/escapes preserved verbatim) and exposed as {@code recBuf[0, recLen)}
     * so the direct-to-block walkers can parse fields straight out of a {@code char[]} without a per-row
     * {@link String} or {@link StringBuilder}. The buffer grows to the largest record seen and is reused
     * across rows; the view is valid only until the next {@link #nextRecord()} call.
     */
    private char[] recBuf;
    private int recLen;

    CsvLogicalRecordReader(Reader reader, char quoteChar, char delimiter, int maxRecordBytes, Charset charset) {
        this(reader, quoteChar, delimiter, maxRecordBytes, charset, true);
    }

    CsvLogicalRecordReader(Reader reader, char quoteChar, char delimiter, int maxRecordBytes, Charset charset, boolean quoteAware) {
        this(reader, quoteChar, delimiter, CsvFormatOptions.DEFAULT_ESCAPE, maxRecordBytes, charset, quoteAware, false);
    }

    CsvLogicalRecordReader(
        Reader reader,
        char quoteChar,
        char delimiter,
        char escapeChar,
        int maxRecordBytes,
        Charset charset,
        boolean quoteAware,
        boolean escapeAware
    ) {
        if (reader.markSupported() == false) {
            throw new IllegalArgumentException("Reader must support mark/reset");
        }
        if (maxRecordBytes <= 0) {
            throw new IllegalArgumentException("maxRecordBytes must be positive, got: " + maxRecordBytes);
        }
        this.reader = reader;
        this.quoteChar = quoteChar;
        this.delimiter = delimiter;
        this.escapeChar = escapeChar;
        this.maxRecordBytes = maxRecordBytes;
        this.charset = charset;
        this.utf8 = StandardCharsets.UTF_8.equals(charset);
        this.quoteAware = quoteAware;
        this.escapeAware = escapeAware;
    }

    /**
     * Makes this reader fail, with a client error, on a record that spans a line terminator (a quoted field or an escape
     * holding one). Armed on every read of a dataset that does not declare {@code multiline_fields: true}: its files are
     * cut at plain newlines on the contract that no record spans one, so such a record breaks the contract and makes the
     * splits unreliable. The reads that cut nothing (the schema sample, a file read in one piece) arm it too, so the same
     * file fails the same way whatever its size and however it is read.
     *
     * <p>Within a cut file this is a best-effort guard, not a proof. A cut that lands inside a multi-line quoted value is
     * reported by the split that owns the value's start (its record runs into the cut newline), so a scan that reads every
     * split fails loudly. A split that starts inside the value cannot recognise that on its own: a scan that stops early
     * (a satisfied {@code LIMIT}) may return rows built from the value's tail before the owning split has been read.
     */
    CsvLogicalRecordReader failOnLineSpan() {
        failOnLineSpan = true;
        return this;
    }

    /**
     * Discards the partial record a byte-range split (bzip2 block, zstd frame) starts in the middle of: the previous split
     * read past its end to finish it, so it is not this split's.
     * <p>
     * Under the line-span guard the record ends at the first line terminator, by the contract the guard enforces, so it is
     * discarded without quote or escape state: the tail of a quoted value may start with its closing quote, which a
     * quote-aware read would take for an opening one and swallow the rows up to the next quote. Without the guard the
     * record is read as the previous split read it, quote-aware.
     */
    void discardLeadingPartialRecord(boolean bracketAware) throws IOException {
        if (failOnLineSpan) {
            discardLine();
        } else {
            readRecord(bracketAware);
        }
    }

    /**
     * Consumes the input up to and including the next line terminator, with no quote, escape or bracket state, committing
     * the bytes to {@link #bytesRead} like {@link #readRecord(boolean)} does so the offsets of the records after it stay
     * anchored. An over-cap line is drained and reported by {@link #addBytes} as for any record.
     */
    private void discardLine() throws IOException {
        int lineBytes = 0;
        boolean terminated = false;
        int ch;
        while ((ch = readChar()) != -1) {
            lineBytes = addBytes(lineBytes, ch);
            if (ch == '\n') {
                terminated = true;
                break;
            }
            if (ch == '\r') {
                int next = readChar();
                if (next == '\n') {
                    lineBytes = addBytes(lineBytes, next);
                } else if (next != -1) {
                    pushBack(next);
                }
                terminated = true;
                break;
            }
        }
        if (lineBytes > 0) {
            this.lastRecordBytes = lineBytes;
            this.bytesRead += lineBytes;
            this.lastRecordTerminated = terminated;
        }
    }

    /**
     * Marks the failure as a record that spans a line terminator while the file is read on the contract that none does. It
     * is the cause of the client error so that the row-error handling of the record-iterator path, which would otherwise
     * count the error as a skipped row and carry on with the tail of the value, can recognise it and rethrow whatever the
     * {@code error_mode}.
     */
    static final class LineSpanningRecordException extends RuntimeException {
        LineSpanningRecordException() {
            super("a record spans several lines while [multiline_fields] is not [true]", null, false, false);
        }
    }

    private static final String LINE_SPANNING_RECORD_MESSAGE = "A quoted field or an escaped line break spans several lines, "
        + "but [multiline_fields] is not [true], so files are read and split on the assumption that no value does. Set "
        + "[multiline_fields] to [true] to read such values";

    private void failIfLineBreak(int ch) {
        if (failOnLineSpan && (ch == '\n' || ch == '\r')) {
            throw ExternalFailures.rowError(new LineSpanningRecordException(), LINE_SPANNING_RECORD_MESSAGE);
        }
    }

    String readRecord(boolean bracketAware) throws IOException {
        StringBuilder sb = new StringBuilder(256);
        boolean inQuotes = false;
        int bracketDepth = 0;
        boolean fieldHasNonWhitespace = false;
        int recordBytes = 0;

        while (true) {
            int ch = readChar();
            if (ch == -1) {
                if (sb.length() == 0) {
                    // True EOF before any character was consumed for this call. Leave
                    // lastRecordBytes/bytesRead untouched so callers see "no record produced".
                    return null;
                }
                // Trailing unterminated record at EOF: the bytes are real input, count them so
                // a subsequent caller's offset arithmetic stays anchored.
                this.lastRecordBytes = recordBytes;
                this.bytesRead += recordBytes;
                this.lastRecordTerminated = false;
                return sb.toString();
            }
            recordBytes = addBytes(recordBytes, ch, inQuotes);

            if (inQuotes) {
                if (escapeAware && ch == escapeChar) {
                    // Carry the escape and the char it escapes verbatim so an escaped quote does not
                    // close the field; the field-level walker performs the actual C-style decode.
                    sb.append((char) ch);
                    int next = readChar();
                    if (next != -1) {
                        failIfLineBreak(next);
                        recordBytes = addBytes(recordBytes, next, true);
                        sb.append((char) next);
                    }
                    continue;
                }
                if (ch == quoteChar) {
                    int next = readChar();
                    if (next == quoteChar) {
                        recordBytes = addBytes(recordBytes, next, true);
                        sb.append((char) ch);
                        sb.append((char) next);
                        continue;
                    }
                    if (next != -1) {
                        pushBack(next);
                    }
                    inQuotes = false;
                    sb.append((char) ch);
                    continue;
                }
                failIfLineBreak(ch);
                sb.append((char) ch);
                continue;
            }

            if (bracketDepth > 0) {
                if (ch == '[') {
                    bracketDepth++;
                } else if (ch == ']') {
                    bracketDepth--;
                    if (bracketDepth == 0) {
                        sb.append((char) ch);
                        fieldHasNonWhitespace = true;
                        continue;
                    }
                }
                sb.append((char) ch);
                continue;
            }

            if (escapeAware && ch == escapeChar) {
                // Outside quotes, an escaped terminator is in-field content (Jackson treats \\+\n as a
                // literal newline), so consume the escaped char verbatim instead of ending the record.
                sb.append((char) ch);
                int next = readChar();
                if (next != -1) {
                    failIfLineBreak(next);
                    recordBytes = addBytes(recordBytes, next);
                    sb.append((char) next);
                }
                fieldHasNonWhitespace = true;
                continue;
            }
            if (ch == '\n') {
                this.lastRecordBytes = recordBytes;
                this.bytesRead += recordBytes;
                this.lastRecordTerminated = true;
                return sb.toString();
            }
            if (ch == '\r') {
                int next = readChar();
                if (next == '\n') {
                    recordBytes = addBytes(recordBytes, next);
                } else if (next != -1) {
                    pushBack(next);
                }
                this.lastRecordBytes = recordBytes;
                this.bytesRead += recordBytes;
                this.lastRecordTerminated = true;
                return sb.toString();
            }
            if (ch == delimiter) {
                sb.append((char) ch);
                fieldHasNonWhitespace = false;
                continue;
            }
            if (quoteAware && ch == quoteChar && fieldHasNonWhitespace == false) {
                inQuotes = true;
                sb.append((char) ch);
                continue;
            }
            if (bracketAware && ch == '[' && fieldHasNonWhitespace == false) {
                bracketDepth = 1;
                sb.append((char) ch);
                continue;
            }
            if (CsvFormatReader.isAsciiCsvFieldLeadingWhitespace(ch) == false) {
                fieldHasNonWhitespace = true;
            }
            sb.append((char) ch);
        }
    }

    /**
     * Enables read-ahead bulk buffering. Only safe when this reader owns the underlying stream for the
     * rest of the read (direct-to-block, or the house per-record path); see the {@link #inBuf} field
     * comment. Returns {@code this} for fluent construction.
     */
    CsvLogicalRecordReader enableBulkBuffering() {
        bulkBuffered = true;
        inBuf = new char[INPUT_BUFFER_SIZE];
        recBuf = new char[256];
        return this;
    }

    /**
     * Bulk-mode, zero-{@link String} sibling of {@link #readRecord(boolean)} for the non-bracket
     * direct-to-block path. Scans the next logical record into the reusable {@link #recBuf} and reports
     * whether one was available. After {@code true} the record is {@code recordBuffer()[0, recordLength())},
     * with the trailing terminator stripped and quotes/escapes preserved verbatim, exactly as the
     * {@code String} returned by {@link #readRecord(boolean)} would have been. The view is overwritten by
     * the next call. Bracket multi-value syntax is never eligible for the direct path, so (unlike
     * {@link #readRecord(boolean)}) there is no bracket-depth tracking here.
     *
     * <p>Like {@link #readRecord(boolean)}, the success path commits {@link #bytesRead} and
     * {@link #lastRecordBytes} on every returned record, using the identical per-record {@code recordBytes}
     * ({@link #addBytes} / UTF-8 inference) as {@code readRecord}. This makes
     * {@code splitStartByte + bytesRead() - lastRecordBytes()} the SAME byte-exact per-record offset axis on
     * the direct-to-block path as on the {@code readRecord}-backed path, so the direct path can harvest
     * canonical-stripe statistics with byte-exact per-record attribution (the emit-time byte-exactness
     * tripwire safe-misses on any skew). Over-cap drains and blank/comment skips also flow through here, so
     * the running offset stays monotonic and exact across skipped records.
     *
     * @return {@code true} if a record was read, {@code false} at end of stream
     * @throws CsvRecordTooLargeException if the record exceeds the configured byte cap
     */
    boolean nextRecord() throws IOException {
        assert bulkBuffered : "nextRecord() requires bulk buffering";
        recLen = 0;
        boolean inQuotes = false;
        boolean fieldHasNonWhitespace = false;
        int recordBytes = 0;

        while (true) {
            int ch = nextBulkChar();
            if (ch == -1) {
                // Trailing unterminated record at EOF: commit its bytes so a subsequent caller's offset
                // arithmetic stays anchored, mirroring readRecord()'s EOF branch. Gate on recordBytes (not
                // recLen) so a trailing record whose only content is a stripped terminator is not under-counted.
                if (recordBytes > 0) {
                    this.lastRecordBytes = recordBytes;
                    this.bytesRead += recordBytes;
                }
                return recLen != 0;
            }
            recordBytes = addBytes(recordBytes, ch, inQuotes);

            if (inQuotes) {
                if (escapeAware && ch == escapeChar) {
                    appendRecord((char) ch);
                    int next = nextBulkChar();
                    if (next != -1) {
                        failIfLineBreak(next);
                        recordBytes = addBytes(recordBytes, next, true);
                        appendRecord((char) next);
                    }
                    continue;
                }
                if (ch == quoteChar) {
                    int next = nextBulkChar();
                    if (next == quoteChar) {
                        recordBytes = addBytes(recordBytes, next, true);
                        appendRecord((char) ch);
                        appendRecord((char) next);
                        continue;
                    }
                    if (next != -1) {
                        pushBack(next);
                    }
                    inQuotes = false;
                    appendRecord((char) ch);
                    continue;
                }
                failIfLineBreak(ch);
                appendRecord((char) ch);
                continue;
            }

            if (escapeAware && ch == escapeChar) {
                appendRecord((char) ch);
                int next = nextBulkChar();
                if (next != -1) {
                    failIfLineBreak(next);
                    recordBytes = addBytes(recordBytes, next);
                    appendRecord((char) next);
                }
                fieldHasNonWhitespace = true;
                continue;
            }
            if (ch == '\n') {
                this.lastRecordBytes = recordBytes;
                this.bytesRead += recordBytes;
                return true;
            }
            if (ch == '\r') {
                int next = nextBulkChar();
                if (next == '\n') {
                    recordBytes = addBytes(recordBytes, next);
                } else if (next != -1) {
                    pushBack(next);
                }
                this.lastRecordBytes = recordBytes;
                this.bytesRead += recordBytes;
                return true;
            }
            if (ch == delimiter) {
                appendRecord((char) ch);
                fieldHasNonWhitespace = false;
                continue;
            }
            if (quoteAware && ch == quoteChar && fieldHasNonWhitespace == false) {
                inQuotes = true;
                appendRecord((char) ch);
                continue;
            }
            if (CsvFormatReader.isAsciiCsvFieldLeadingWhitespace(ch) == false) {
                fieldHasNonWhitespace = true;
            }
            appendRecord((char) ch);
        }
    }

    /** Backing buffer of the record exposed by the most recent {@link #nextRecord()} (valid for {@code [0, recordLength())}). */
    char[] recordBuffer() {
        return recBuf;
    }

    /** Length of the record exposed by the most recent {@link #nextRecord()}. */
    int recordLength() {
        return recLen;
    }

    private void appendRecord(char c) {
        if (recLen == recBuf.length) {
            recBuf = Arrays.copyOf(recBuf, recBuf.length * 2);
        }
        recBuf[recLen++] = c;
    }

    /**
     * Bulk-mode next character with the common in-buffer load as the first, inlinable branch. The
     * pushed-back lookahead and the (rare) chunk refill are handled out of line so the hot per-character
     * path is just a pending check and an array load. Used only by {@link #nextRecord()}.
     */
    private int nextBulkChar() throws IOException {
        if (pending == NO_PENDING && inPos < inLimit) {
            return inBuf[inPos++];
        }
        return nextBulkCharSlow();
    }

    private int nextBulkCharSlow() throws IOException {
        if (pending != NO_PENDING) {
            int c = pending;
            pending = NO_PENDING;
            return c;
        }
        if (eof) {
            return -1;
        }
        inLimit = reader.read(inBuf, 0, inBuf.length);
        inPos = 0;
        if (inLimit <= 0) {
            eof = true;
            return -1;
        }
        return inBuf[inPos++];
    }

    /**
     * Returns the next character, or {@code -1} at end of stream. In bulk mode it serves a pushed-back
     * lookahead first, then pulls from {@link #inBuf}, refilling once per chunk rather than once per
     * character. In non-bulk mode it reads exactly one character from the underlying reader, first
     * setting a one-character {@code mark} so {@link #pushBack(int)} can un-read the lookahead and leave
     * the underlying reader positioned for whoever resumes (e.g. the Jackson tokenizer).
     */
    private int readChar() throws IOException {
        if (bulkBuffered) {
            return nextBulkChar();
        }
        reader.mark(1);
        return reader.read();
    }

    /**
     * Pushes a single look-ahead character back so the next {@link #readChar()} re-returns it. In bulk
     * mode the character is held in {@link #pending}; in non-bulk mode the underlying reader is
     * {@code reset} to the mark taken before the look-ahead read, so the character is returned to the
     * shared stream rather than consumed.
     */
    private void pushBack(int c) throws IOException {
        if (bulkBuffered) {
            pending = c;
        } else {
            reader.reset();
        }
    }

    /**
     * Encoded-byte length of the most recently returned record (line terminator included). Zero
     * before any {@code readRecord} call returns successfully and never updated by the
     * {@code null}-returning EOF path. A record's file-relative start offset is
     * {@code bytesRead() - lastRecordBytes()}.
     */
    int lastRecordBytes() {
        return lastRecordBytes;
    }

    /**
     * Whether the record {@link #readRecord(boolean)} last returned ended with a line terminator. A record without one
     * ran into the end of the input: the end of the file, or the end of a range or chunk that cut the record short.
     */
    boolean lastRecordTerminated() {
        return lastRecordTerminated;
    }

    /**
     * The stream this reader pulls from, positioned at the next character it would return. Only valid while this reader
     * has not read ahead: the leading-record reader never enables bulk buffering, and a completed record leaves no
     * pushed-back character.
     */
    Reader source() {
        assert bulkBuffered == false : "a bulk-buffered reader has read ahead of its source";
        assert pending == NO_PENDING : "a pushed-back character is not on the source";
        return reader;
    }

    /**
     * Cumulative encoded-byte count of all records this reader has returned. Mirrors NDJSON's
     * {@code JsonParser.getCurrentLocation().getByteOffset()} so a split-relative byte offset
     * survives multi-split layouts: the same physical record sits at the same
     * {@code splitStartByte + bytesRead - lastRecordBytes} regardless of where the split boundary
     * lands.
     */
    long bytesRead() {
        return bytesRead;
    }

    /**
     * Seeds the cumulative byte counter to {@code offset} before any records are read. Used when
     * bytes have been consumed from the underlying stream before this reader was constructed (e.g.
     * a UTF-8 BOM stripped from the raw stream) so that {@code splitStartByte + bytesRead() -
     * lastRecordBytes()} still yields the correct file-global byte position for every record.
     * Must be called before the first {@link #readRecord} invocation.
     */
    void setInitialByteOffset(long offset) {
        assert bytesRead == 0L : "initial offset must be set before any records are read";
        bytesRead = offset;
    }

    private int addBytes(int recordBytes, int ch) throws CsvRecordTooLargeException, IOException {
        return addBytes(recordBytes, ch, false);
    }

    /**
     * Counts {@code ch} toward the record cap. {@code inQuotes} is the quote state before {@code ch} is applied.
     * A newline inside a quoted field, while the line-span guard is armed, fails that contract before the cap is
     * considered, and a cap tripped inside the field still fails it when the newline comes before the quote closes.
     * Otherwise an oversized record is drained to the next physical line and reported as too large, so a lenient
     * {@code error_mode} can resume at the next record.
     */
    private int addBytes(int recordBytes, int ch, boolean inQuotes) throws CsvRecordTooLargeException, IOException {
        if (inQuotes) {
            failIfLineBreak(ch);
        }
        // Hot ASCII fast path: under UTF-8 every code unit <= 0x7f is exactly one byte, which is the
        // overwhelming majority of CSV content, so skip the encodedLength/charset call chain for it.
        int next = recordBytes + encodedWidth(ch);
        if (next > maxRecordBytes) {
            // Oversized record. Drain to the end of the physical line so the reader is left at the
            // next record's first byte and bytesRead counts the whole line. The lenient error policy
            // skips this record and resumes from the next read; without draining it would resume
            // mid-line and every later _rowPosition offset would be short by the undrained tail
            // (and could collide with an earlier record's offset). lastRecordBytes is left untouched:
            // the caller's exception handler treats this as "no record produced". The drain reads
            // through readChar()/pushBack(), not the underlying reader directly, so in bulk mode it
            // consumes any chars already pulled into inBuf/pending rather than skipping past them and
            // desyncing the stream (which would leave the next nextRecord() resuming mid-line).
            // While the line-span guard is armed, a newline that is still inside the quoted field (or a
            // backslash-escaped newline) fails the contract instead, so the tail is not read as further rows.
            next = drainOverCap(next, ch, inQuotes);
            this.bytesRead += next;
            throw new CsvRecordTooLargeException(maxRecordBytes);
        }
        return next;
    }

    private int encodedWidth(int ch) {
        return utf8 && ch <= 0x7f ? 1 : encodedLength(ch);
    }

    /**
     * Finishes an over-cap record whose tipping character {@code ch} is already included in {@code counted}.
     * Returns the total bytes consumed through the record's terminator.
     */
    private int drainOverCap(int counted, int ch, boolean inQuotes) throws IOException {
        if (failOnLineSpan == false) {
            return drainPhysicalLine(counted, ch);
        }
        if (inQuotes) {
            return drainOpenQuote(counted, ch);
        }
        if (escapeAware) {
            return drainPhysicalLineAware(counted, ch);
        }
        return drainPhysicalLine(counted, ch);
    }

    /** Quote-blind drain used when the line-span guard is off. {@code ch} is already included in {@code counted}. */
    private int drainPhysicalLine(int counted, int ch) throws IOException {
        if (ch == '\r') {
            return drainCarriageReturn(counted);
        }
        if (ch == '\n') {
            return counted;
        }
        int c;
        while ((c = readChar()) != -1) {
            counted += encodedLength(c);
            if (c == '\n') {
                return counted;
            }
            if (c == '\r') {
                return drainCarriageReturn(counted);
            }
        }
        return counted;
    }

    /**
     * Drains from inside an open quoted field. A line break before the quote closes fails the line-span contract.
     * Once the quote closes, the rest of the line is a single-line record that is only too large.
     * {@code ch} is already included in {@code counted} and has not yet been applied to the quote state.
     */
    private int drainOpenQuote(int counted, int ch) throws IOException {
        int current = ch;
        boolean pendingEscape = false;
        while (true) {
            if (pendingEscape) {
                pendingEscape = false;
                failIfLineBreak(current);
            } else if (escapeAware && current == escapeChar) {
                pendingEscape = true;
            } else if (current == quoteChar) {
                int next = readChar();
                if (next == -1) {
                    return counted;
                }
                counted += encodedWidth(next);
                if (next != quoteChar) {
                    return drainPhysicalLineAware(counted, next);
                }
            } else {
                failIfLineBreak(current);
            }
            int next = readChar();
            if (next == -1) {
                return counted;
            }
            current = next;
            counted += encodedWidth(next);
        }
    }

    /**
     * Drains to the next unescaped line terminator. A backslash-escaped newline fails the line-span contract.
     * {@code ch} is already included in {@code counted}.
     */
    private int drainPhysicalLineAware(int counted, int ch) throws IOException {
        int current = ch;
        boolean pendingEscape = false;
        while (true) {
            if (pendingEscape) {
                pendingEscape = false;
                failIfLineBreak(current);
            } else if (escapeAware && current == escapeChar) {
                pendingEscape = true;
            } else if (current == '\n') {
                return counted;
            } else if (current == '\r') {
                return drainCarriageReturn(counted);
            }
            int next = readChar();
            if (next == -1) {
                return counted;
            }
            current = next;
            counted += encodedWidth(next);
        }
    }

    /**
     * Consume an optional {@code \n} immediately following a {@code \r} (so {@code \r\n} counts as
     * one terminator), returning the running byte count. The reader is positioned at the next
     * record's first byte on return.
     */
    private int drainCarriageReturn(int byteCount) throws IOException {
        int peek = readChar();
        if (peek == '\n') {
            return byteCount + encodedLength(peek);
        }
        if (peek != -1) {
            pushBack(peek);
        }
        return byteCount;
    }

    private int encodedLength(int ch) {
        if (utf8) {
            return utf8Length(ch);
        }
        return String.valueOf((char) ch).getBytes(charset).length;
    }

    private static int utf8Length(int ch) {
        if (ch <= 0x7f) {
            return 1;
        }
        if (ch <= 0x7ff) {
            return 2;
        }
        if (Character.isSurrogate((char) ch)) {
            return 2;
        }
        return 3;
    }
}
