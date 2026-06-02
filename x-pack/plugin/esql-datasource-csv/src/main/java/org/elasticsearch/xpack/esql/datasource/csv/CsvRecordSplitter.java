/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

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
     * Quoting rule mirrors the tokenizer {@link CsvFormatReader#readCsvRecord}: a {@code quoteChar} opens a quoted field
     * only at field start (after an unquoted {@code delimiter} or {@code \n}, optionally past field-leading
     * whitespace); a mid-field {@code quoteChar} is a literal and does not toggle quote state.
     * <p>
     * Best-effort/open-tail contract: the scan assumes the buffer begins at a record boundary and advances
     * {@code lastBoundary} only on a true unquoted record terminator. So a chunk the segmentator cut mid-record
     * yields no boundary inside that leading partial, and a genuinely unterminated quoted field keeps
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
        byte quoteAsByte = (byte) options.quoteChar();
        byte delimAsByte = (byte) options.delimiter();
        for (int i = offset; i < end; i++) {
            byte b = buf[i];
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
            } else if (b == quoteAsByte && fieldHasNonWhitespace == false) {
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
     * Applies the same field-start quoting rule as the actual tokenizer {@link CsvFormatReader#readCsvRecord}
     * and as {@link #findNextRecordBoundaryBracketCommaMvc}: a {@code quoteChar} opens a quoted field only at
     * field start (optionally after field-leading whitespace); a mid-field {@code quoteChar} is a
     * literal and does not toggle quote state. Returns the byte count up to and including the first
     * record-terminating {@code \n} that is outside a quoted field, or {@code -1} at EOF.
     */
    private long findNextRecordBoundaryQuotedFieldsOnly(InputStream stream) throws IOException {
        BufferedInputStream bis = stream instanceof BufferedInputStream b ? b : new BufferedInputStream(stream);
        long consumed = 0;
        boolean inQuotes = false;
        boolean fieldHasNonWhitespace = false;
        byte quoteAsByte = (byte) options.quoteChar();
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
            } else if (b == quoteAsByte && fieldHasNonWhitespace == false) {
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
