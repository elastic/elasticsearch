/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringWriter;

import static org.hamcrest.Matchers.equalTo;

public class ChunkedTextWriterTests extends ESTestCase {

    public void testTrimmedStringMatchesStringTrim() throws IOException {
        for (int i = 0; i < 100; i++) {
            String text = randomText();
            ChunkedTextWriter writer = new ChunkedTextWriter(randomChunkSize());
            writeInRandomPieces(writer, text);
            assertThat(writer.length(), equalTo((long) text.length()));
            assertThat(writer.toTrimmedString(), equalTo(text.trim()));
        }
    }

    public void testEmpty() {
        ChunkedTextWriter writer = new ChunkedTextWriter(randomChunkSize());
        assertThat(writer.length(), equalTo(0L));
        assertThat(writer.toTrimmedString(), equalTo(""));
    }

    public void testWhitespaceOnly() throws IOException {
        String text = randomWhitespace(between(1, 100));
        ChunkedTextWriter writer = new ChunkedTextWriter(randomChunkSize());
        writeInRandomPieces(writer, text);
        assertThat(writer.length(), equalTo((long) text.length()));
        assertThat(writer.toTrimmedString(), equalTo(""));
    }

    public void testSurrogatePairSplitAcrossChunks() throws IOException {
        String emoji = "😀"; // a single code point that takes two chars
        String text = "a" + emoji + "b" + emoji;
        // a chunk size of 2 splits the first pair across two chunks
        ChunkedTextWriter writer = new ChunkedTextWriter(2);
        writeInRandomPieces(writer, text);
        String result = writer.toTrimmedString();
        assertThat(result, equalTo(text));
        assertThat(result.codePointCount(0, result.length()), equalTo(4));
    }

    public void testWriteToWritesPrefix() throws IOException {
        for (int i = 0; i < 100; i++) {
            String text = randomText();
            ChunkedTextWriter writer = new ChunkedTextWriter(randomChunkSize());
            writeInRandomPieces(writer, text);

            int maxChars = randomBoolean() ? -1 : between(0, text.length() + 10);
            StringWriter out = new StringWriter();
            writer.writeTo(out, maxChars);
            String expected = maxChars < 0 ? text : text.substring(0, Math.min(maxChars, text.length()));
            assertThat(out.toString(), equalTo(expected));

            // writing the text out leaves the writer as it was
            assertThat(writer.length(), equalTo((long) text.length()));
            String more = randomText();
            writeInRandomPieces(writer, more);
            assertThat(writer.toTrimmedString(), equalTo((text + more).trim()));
        }
    }

    public void testUnusableAfterCollecting() {
        ChunkedTextWriter writer = new ChunkedTextWriter(randomChunkSize());
        writer.write("text", 0, 4);
        assertThat(writer.toTrimmedString(), equalTo("text"));
        expectThrows(IllegalStateException.class, () -> writer.write("more", 0, 4));
        expectThrows(IllegalStateException.class, writer::toTrimmedString);
        expectThrows(IllegalStateException.class, () -> writer.writeTo(new StringWriter(), -1));
    }

    public void testRejectsInvalidRange() {
        ChunkedTextWriter writer = new ChunkedTextWriter(randomChunkSize());
        expectThrows(IndexOutOfBoundsException.class, () -> writer.write(new char[4], 0, between(-10, -1)));
        expectThrows(IndexOutOfBoundsException.class, () -> writer.write(new char[4], 1, 4));
        expectThrows(IndexOutOfBoundsException.class, () -> writer.write("text", 0, between(-10, -1)));
        expectThrows(IndexOutOfBoundsException.class, () -> writer.write("text", 1, 4));
        assertThat(writer.length(), equalTo(0L));
    }

    public void testInvalidChunkSize() {
        expectThrows(IllegalArgumentException.class, () -> new ChunkedTextWriter(between(-10, 0)));
    }

    private static int randomChunkSize() {
        return randomFrom(
            1,
            2,
            3,
            between(1, 64),
            between(ChunkedTextWriter.INITIAL_BUFFER_SIZE, 1000),
            ChunkedTextWriter.DEFAULT_CHUNK_SIZE
        );
    }

    /** Random whitespace at both ends (possibly spanning chunks) around a middle mixing Latin-1 and non-Latin-1 chars. */
    private static String randomText() {
        StringBuilder sb = new StringBuilder();
        sb.append(randomWhitespace(between(0, 20)));
        int middle = between(0, 2000);
        for (int i = 0; i < middle; i++) {
            switch (between(0, 4)) {
                case 0 -> sb.append(randomAlphaOfLength(1));
                case 1 -> sb.append(' ');
                case 2 -> sb.append((char) between(0xA0, 0xFF)); // Latin-1
                case 3 -> sb.append(randomFrom('€', 'ł', 'Ж', '中'));
                case 4 -> sb.append("😀");
                default -> throw new AssertionError();
            }
        }
        sb.append(randomWhitespace(between(0, 20)));
        return sb.toString();
    }

    /** Chars that {@link String#trim()} removes: everything up to and including the space. */
    private static String randomWhitespace(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(randomFrom(' ', '\n', '\t', '\r', '\u0000', (char) between(0, ' ')));
        }
        return sb.toString();
    }

    private static void writeInRandomPieces(ChunkedTextWriter writer, String text) throws IOException {
        int off = 0;
        while (off < text.length()) {
            int len = between(1, text.length() - off);
            switch (between(0, 2)) {
                case 0 -> writer.write(text, off, len);
                case 1 -> {
                    // with an offset into a larger array, as Tika's SAX handlers do
                    char[] chars = ("xx" + text.substring(off, off + len) + "yy").toCharArray();
                    writer.write(chars, 2, len);
                }
                case 2 -> {
                    len = 1;
                    writer.write(text.charAt(off));
                }
                default -> throw new AssertionError();
            }
            off += len;
        }
    }
}
