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
                return sb.length() == 0 ? null : sb.toString();
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

    private int addBytes(int recordBytes, int ch) throws CsvRecordTooLargeException {
        int next = recordBytes + encodedLength(ch);
        if (next > maxRecordBytes) {
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
}
