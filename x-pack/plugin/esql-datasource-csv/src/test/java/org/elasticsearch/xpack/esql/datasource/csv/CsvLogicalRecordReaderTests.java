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
import java.util.ArrayList;
import java.util.List;

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
        assertEquals("record exceeds [5b], possibly an unclosed quote or bracket", ex.getMessage());
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

    /**
     * The bulk {@code nextRecord()} path serves most records as a window on its input buffer and copies only a
     * record that a refill interrupts. Both must equal what {@code readRecord} returns, record for record, with the
     * same byte accounting, whatever falls on the 8K buffer boundary: a plain field, a quoted field with an escaped
     * quote, a lone CR whose LF check reads into the next fill, a record longer than the buffer, or a pending
     * lookahead at the start of a record.
     */
    public void testBulkRecordsMatchStringRecordsAcrossBufferBoundaries() throws IOException {
        StringBuilder input = new StringBuilder();
        java.util.Random random = new java.util.Random(randomLong());
        int records = 0;
        while (input.length() < 40 * 8192) {
            int shape = random.nextInt(8);
            switch (shape) {
                case 0 -> input.append("id").append(records).append(",\"quoted \"\"q\"\" value\",tail");
                case 1 -> input.append("a,b,").append("x".repeat(random.nextInt(60)));
                case 2 -> input.append("\"multi\nline\r\nfield\",").append(records);
                case 3 -> input.append("y".repeat(8192 + random.nextInt(3000)));
                case 4 -> input.append("é,ü,").append(records);
                default -> input.append(records).append(",").append(random.nextInt(1000)).append(",plain");
            }
            input.append(switch (random.nextInt(3)) {
                case 0 -> "\n";
                case 1 -> "\r\n";
                default -> "\r";
            });
            records++;
        }
        String text = input.toString();
        CsvLogicalRecordReader expected = newReader(text, StandardCharsets.UTF_8);
        CsvLogicalRecordReader bulk = newReader(text, StandardCharsets.UTF_8).enableBulkBuffering();
        int seen = 0;
        String record;
        while ((record = expected.readRecord(false)) != null) {
            assertTrue("record " + seen, bulk.nextRecord());
            String actual = new String(bulk.recordBuffer(), bulk.recordOffset(), bulk.recordLength());
            assertEquals("record " + seen, record, actual);
            assertEquals("record " + seen + " bytes", expected.lastRecordBytes(), bulk.lastRecordBytes());
            assertEquals("record " + seen + " offset", expected.bytesRead(), bulk.bytesRead());
            assertPlainFieldEnds(record, bulk);
            seen++;
        }
        assertFalse(bulk.nextRecord());
        assertEquals(records, seen);
    }

    /**
     * A record without a quote or escape character reports every delimiter as a field end; one with either reports
     * none, leaving the split to the caller.
     */
    private static void assertPlainFieldEnds(String record, CsvLogicalRecordReader bulk) {
        int[] ends = bulk.plainFieldEnds();
        if (record.indexOf('"') >= 0) {
            assertNull(record, ends);
            return;
        }
        assertNotNull(record, ends);
        List<Integer> expectedEnds = new ArrayList<>();
        for (int i = 0; i < record.length(); i++) {
            if (record.charAt(i) == ',') {
                expectedEnds.add(i);
            }
        }
        List<Integer> actualEnds = new ArrayList<>();
        for (int i = 0; i < bulk.plainFieldEndCount(); i++) {
            actualEnds.add(ends[i]);
        }
        assertEquals(record, expectedEnds, actualEnds);
    }

    public void testPlainFieldEndsWithoutBulkFastPath() throws IOException {
        // A non-UTF-8 charset has no bulk scan, so the per-character branches record the field ends.
        CsvLogicalRecordReader reader = newReader("a,b,,d\n\"q\",x\ne,f\n", StandardCharsets.ISO_8859_1).enableBulkBuffering();
        assertTrue(reader.nextRecord());
        assertPlainFieldEnds("a,b,,d", reader);
        assertTrue(reader.nextRecord());
        assertNull(reader.plainFieldEnds());
        assertTrue(reader.nextRecord());
        assertPlainFieldEnds("e,f", reader);
        assertFalse(reader.nextRecord());
    }
}
