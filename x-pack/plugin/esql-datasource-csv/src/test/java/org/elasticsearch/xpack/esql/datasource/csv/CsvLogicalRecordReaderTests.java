/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Pins the per-record byte accounting that {@link CsvLogicalRecordReader} exposes via
 * {@link CsvLogicalRecordReader#lastRecordBytes()} / {@link CsvLogicalRecordReader#bytesRead()}.
 * The byte arithmetic feeds the {@code _rowPosition} channel on multi-split CSV reads, so a
 * regression in encoded-byte counting here surfaces as drifting {@code _id} values upstream.
 */
public class CsvLogicalRecordReaderTests extends ESTestCase {

    private static final int MAX_BYTES = 1 << 16;

    public void testAsciiUtf8RecordsCountOneBytePerChar() throws IOException {
        // 3 rows; each terminated by '\n'. Every character is ASCII so encoded length equals char length.
        String input = "a,b,c\n1,2,3\nfoo,bar,baz\n";
        CsvLogicalRecordReader reader = newReader(input, StandardCharsets.UTF_8);

        String r1 = reader.readRecord(false);
        assertEquals("a,b,c", r1);
        assertEquals(6, reader.lastRecordBytes()); // 5 chars + '\n'
        assertEquals(6L, reader.bytesRead());

        String r2 = reader.readRecord(false);
        assertEquals("1,2,3", r2);
        assertEquals(6, reader.lastRecordBytes());
        assertEquals(12L, reader.bytesRead());

        String r3 = reader.readRecord(false);
        assertEquals("foo,bar,baz", r3);
        assertEquals(12, reader.lastRecordBytes()); // 11 chars + '\n'
        assertEquals(24L, reader.bytesRead());

        assertNull(reader.readRecord(false));
        // lastRecordBytes / bytesRead unchanged after the null EOF return.
        assertEquals(12, reader.lastRecordBytes());
        assertEquals(24L, reader.bytesRead());
    }

    public void testCrlfTerminatorCountsTwoBytes() throws IOException {
        String input = "ab\r\ncd\r\n";
        CsvLogicalRecordReader reader = newReader(input, StandardCharsets.UTF_8);

        assertEquals("ab", reader.readRecord(false));
        assertEquals(4, reader.lastRecordBytes()); // 'a','b','\r','\n'
        assertEquals(4L, reader.bytesRead());

        assertEquals("cd", reader.readRecord(false));
        assertEquals(4, reader.lastRecordBytes());
        assertEquals(8L, reader.bytesRead());
    }

    public void testLoneCarriageReturnCountsOneByte() throws IOException {
        // Classic Mac line ending: '\r' alone terminates without consuming a follow-up byte.
        String input = "ab\rcd\r";
        CsvLogicalRecordReader reader = newReader(input, StandardCharsets.UTF_8);

        assertEquals("ab", reader.readRecord(false));
        assertEquals(3, reader.lastRecordBytes()); // 'a','b','\r'
        assertEquals(3L, reader.bytesRead());

        assertEquals("cd", reader.readRecord(false));
        assertEquals(3, reader.lastRecordBytes());
        assertEquals(6L, reader.bytesRead());
    }

    public void testUnterminatedLastRecordCountsItsBytes() throws IOException {
        String input = "x,y,z";
        CsvLogicalRecordReader reader = newReader(input, StandardCharsets.UTF_8);

        assertEquals("x,y,z", reader.readRecord(false));
        // No line terminator at EOF — bytes count only the characters consumed.
        assertEquals(5, reader.lastRecordBytes());
        assertEquals(5L, reader.bytesRead());

        assertNull(reader.readRecord(false));
        assertEquals(5L, reader.bytesRead());
    }

    public void testMultiByteUtf8RecordsCountEncodedBytes() throws IOException {
        // 'é' is 2 bytes (U+00E9), '€' is 3 bytes (U+20AC) in UTF-8.
        // Row "é,€\n" is 2 + 1 + 3 + 1 = 7 bytes.
        String input = "é,€\nabc\n";
        CsvLogicalRecordReader reader = newReader(input, StandardCharsets.UTF_8);

        assertEquals("é,€", reader.readRecord(false));
        assertEquals(7, reader.lastRecordBytes());
        assertEquals(7L, reader.bytesRead());

        assertEquals("abc", reader.readRecord(false));
        assertEquals(4, reader.lastRecordBytes());
        assertEquals(11L, reader.bytesRead());
    }

    public void testNonUtf8SingleByteCharsetCountsOneBytePerChar() throws IOException {
        // ISO-8859-1: 'é' (U+00E9) is a single byte (0xE9). Total "é,a\n" = 4 bytes.
        Charset latin1 = StandardCharsets.ISO_8859_1;
        String input = "é,a\nbc\n";
        CsvLogicalRecordReader reader = newReader(input, latin1);

        assertEquals("é,a", reader.readRecord(false));
        assertEquals(4, reader.lastRecordBytes());
        assertEquals(4L, reader.bytesRead());

        assertEquals("bc", reader.readRecord(false));
        assertEquals(3, reader.lastRecordBytes());
        assertEquals(7L, reader.bytesRead());
    }

    public void testQuotedFieldWithEmbeddedNewlineCountsEverything() throws IOException {
        // The quoted newline is data, not a terminator. Record "\"a\nb\",x\n" is 8 bytes.
        String input = "\"a\nb\",x\nnext\n";
        CsvLogicalRecordReader reader = newReader(input, StandardCharsets.UTF_8);

        String r1 = reader.readRecord(false);
        assertEquals("\"a\nb\",x", r1);
        assertEquals(8, reader.lastRecordBytes()); // " a \n b " , x \n
        assertEquals(8L, reader.bytesRead());

        assertEquals("next", reader.readRecord(false));
        assertEquals(5, reader.lastRecordBytes());
        assertEquals(13L, reader.bytesRead());
    }

    public void testBytesReadMatchesRawInputForUtf8Ascii() throws IOException {
        String input = "h1,h2,h3\n1,2,3\n4,5,6\n";
        CsvLogicalRecordReader reader = newReader(input, StandardCharsets.UTF_8);

        while (reader.readRecord(false) != null) {
            // drain
        }
        long expected = input.getBytes(StandardCharsets.UTF_8).length;
        assertEquals(expected, reader.bytesRead());
    }

    private static CsvLogicalRecordReader newReader(String input, Charset charset) {
        // Anchor character decoding on the requested charset so the encoded-length arithmetic
        // matches what an InputStreamReader-fed reader would see in production.
        BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(input.getBytes(charset)), charset));
        return new CsvLogicalRecordReader(br, '"', ',', MAX_BYTES, charset);
    }

    public void testStringReaderPathAlsoTracksUtf8Bytes() throws IOException {
        // StringReader operates directly on Java chars (not encoded bytes), but the byte-counting
        // logic inside CsvLogicalRecordReader is derived from the char + charset, so encoded-length
        // accounting is identical to the InputStreamReader path above.
        String input = "ab\n€\n";
        CsvLogicalRecordReader reader = new CsvLogicalRecordReader(
            new BufferedReader(new StringReader(input)),
            '"',
            ',',
            MAX_BYTES,
            StandardCharsets.UTF_8
        );

        assertEquals("ab", reader.readRecord(false));
        assertEquals(3, reader.lastRecordBytes());
        assertEquals(3L, reader.bytesRead());

        assertEquals("€", reader.readRecord(false));
        assertEquals(4, reader.lastRecordBytes()); // 3-byte '€' + 1-byte '\n'
        assertEquals(7L, reader.bytesRead());
    }

    /**
     * When {@code CsvRecordTooLargeException} is thrown for an oversized record, the reader must
     * drain the rest of that physical line before throwing: {@code bytesRead} counts the WHOLE
     * oversized line and the reader is left at the next record's first byte. Otherwise the lenient
     * error policy resumes mid-line — yielding a phantom record from the line's tail and a
     * file-global offset short by the undrained bytes, which produces silently wrong (and possibly
     * colliding) {@code _id} values for every subsequent record. This pins the drain-then-throw.
     */
    public void testRecordTooLargeDrainsLineSoNextRecordOffsetIsAnchored() throws IOException {
        String input = "ab\nabcdefgh\nfin\n";
        CsvLogicalRecordReader reader = new CsvLogicalRecordReader(
            new BufferedReader(new StringReader(input)),
            '"',
            ',',
            5,
            StandardCharsets.UTF_8
        );

        assertEquals("ab", reader.readRecord(false));
        assertEquals(3, reader.lastRecordBytes());
        assertEquals(3L, reader.bytesRead());

        // Second record overflows at the 6th byte ("abcde" = 5 bytes is exactly the cap; 'f' pushes
        // next to 6 > maxRecordBytes). The throw drains the rest of "abcdefgh\n" (9 bytes total), so
        // bytesRead = 3 + 9 = 12 and the reader sits at "fin\n".
        IOException ex = expectThrows(IOException.class, () -> reader.readRecord(false));
        assertEquals("CSV record exceeded max_record_size [5]", ex.getMessage());
        assertEquals("bytesRead must count the whole drained oversized line", 12L, reader.bytesRead());

        // lastRecordBytes intentionally unchanged from the prior successful read — caller's catch
        // treats the failed record as "no record produced".
        assertEquals(3, reader.lastRecordBytes());

        // The next record is the real "fin", NOT a phantom "gh" from the un-drained tail, and its
        // file-global start offset resolves to 12 (immediately after the oversized line).
        assertEquals("fin", reader.readRecord(false));
        assertEquals(4, reader.lastRecordBytes()); // "fin" + '\n'
        assertEquals(16L, reader.bytesRead());
        assertEquals("next record start offset", 12L, reader.bytesRead() - reader.lastRecordBytes());
    }

    /**
     * CRLF variant of {@link #testRecordTooLargeDrainsLineSoNextRecordOffsetIsAnchored}: the drain
     * must treat {@code \r\n} as a single terminator and leave the reader at the next record.
     */
    public void testRecordTooLargeDrainsCrlfTerminatedLine() throws IOException {
        String input = "ab\r\nabcdefgh\r\nfin\r\n";
        CsvLogicalRecordReader reader = new CsvLogicalRecordReader(
            new BufferedReader(new StringReader(input)),
            '"',
            ',',
            5,
            StandardCharsets.UTF_8
        );

        assertEquals("ab", reader.readRecord(false));
        assertEquals(4L, reader.bytesRead()); // "ab\r\n"

        // "abcdefgh\r\n" = 10 bytes; drained whole. bytesRead = 4 + 10 = 14, reader at "fin\r\n".
        expectThrows(IOException.class, () -> reader.readRecord(false));
        assertEquals(14L, reader.bytesRead());

        assertEquals("fin", reader.readRecord(false));
        assertEquals(5, reader.lastRecordBytes()); // "fin\r\n"
        assertEquals("next record start offset", 14L, reader.bytesRead() - reader.lastRecordBytes());
    }
}
