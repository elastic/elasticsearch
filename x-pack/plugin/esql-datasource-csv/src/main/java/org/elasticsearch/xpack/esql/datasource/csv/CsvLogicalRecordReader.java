/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Reads one logical CSV record at a time while enforcing the query's byte-sized record cap.
 */
final class CsvLogicalRecordReader {

    private final Reader reader;
    private final char quoteChar;
    private final char delimiter;
    private final int maxRecordBytes;
    private final Charset charset;
    private final boolean utf8;
    /**
     * Encoded-byte length of the most recently returned record, including its line terminator(s).
     * Set just before each {@link #readRecord(boolean)} return so callers can derive a record's
     * start offset as {@code bytesRead() - lastRecordBytes()} without re-walking the input.
     */
    private int lastRecordBytes = 0;
    /**
     * Cumulative encoded-byte count of every record this reader has returned (including terminators
     * and partial records consumed by the caller — schema discovery, header skipping, mid-record
     * resync). Anchors the file-global byte offset CSV emits on the {@code _rowPosition} channel,
     * mirroring the NDJSON {@code recordFileOffset} computation in
     * {@code NdJsonPageDecoder#recordFileOffset()}.
     */
    private long bytesRead = 0L;

    CsvLogicalRecordReader(Reader reader, char quoteChar, char delimiter, int maxRecordBytes, Charset charset) {
        if (reader.markSupported() == false) {
            throw new IllegalArgumentException("Reader must support mark/reset");
        }
        if (maxRecordBytes <= 0) {
            throw new IllegalArgumentException("maxRecordBytes must be positive, got: " + maxRecordBytes);
        }
        this.reader = reader;
        this.quoteChar = quoteChar;
        this.delimiter = delimiter;
        this.maxRecordBytes = maxRecordBytes;
        this.charset = charset;
        this.utf8 = StandardCharsets.UTF_8.equals(charset);
    }

    String readRecord(boolean bracketAware) throws IOException {
        StringBuilder sb = new StringBuilder(256);
        boolean inQuotes = false;
        int bracketDepth = 0;
        boolean fieldHasNonWhitespace = false;
        int recordBytes = 0;

        while (true) {
            int ch = reader.read();
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
                return sb.toString();
            }
            recordBytes = addBytes(recordBytes, ch);

            if (inQuotes) {
                if (ch == quoteChar) {
                    reader.mark(1);
                    int next = reader.read();
                    if (next == quoteChar) {
                        recordBytes = addBytes(recordBytes, next);
                        sb.append((char) ch);
                        sb.append((char) next);
                        continue;
                    }
                    if (next != -1) {
                        reader.reset();
                    }
                    inQuotes = false;
                    sb.append((char) ch);
                    continue;
                }
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

            if (ch == '\n') {
                this.lastRecordBytes = recordBytes;
                this.bytesRead += recordBytes;
                return sb.toString();
            }
            if (ch == '\r') {
                reader.mark(1);
                int next = reader.read();
                if (next == '\n') {
                    recordBytes = addBytes(recordBytes, next);
                } else if (next != -1) {
                    reader.reset();
                }
                this.lastRecordBytes = recordBytes;
                this.bytesRead += recordBytes;
                return sb.toString();
            }
            if (ch == delimiter) {
                sb.append((char) ch);
                fieldHasNonWhitespace = false;
                continue;
            }
            if (ch == quoteChar && fieldHasNonWhitespace == false) {
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
     * Encoded-byte length of the most recently returned record (line terminator included). Zero
     * before any {@code readRecord} call returns successfully and never updated by the
     * {@code null}-returning EOF path. A record's file-relative start offset is
     * {@code bytesRead() - lastRecordBytes()}.
     */
    int lastRecordBytes() {
        return lastRecordBytes;
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

    private int addBytes(int recordBytes, int ch) throws CsvRecordTooLargeException {
        int next = recordBytes + encodedLength(ch);
        if (next > maxRecordBytes) {
            // Commit consumed bytes (including the overflowing char) to the cumulative counter
            // before throwing so the next record's offset stays anchored. lastRecordBytes is not
            // touched — caller's exception handler treats this as "no record produced".
            this.bytesRead += next;
            throw new CsvRecordTooLargeException(maxRecordBytes);
        }
        return next;
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

    static final class CsvRecordTooLargeException extends IOException {
        CsvRecordTooLargeException(int maxRecordBytes) {
            super("CSV record exceeded max_record_size [" + maxRecordBytes + "]");
        }
    }
}
