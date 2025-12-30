/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.gcs;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.SequencedMap;
import java.util.stream.IntStream;

import static fixture.gcs.MultipartContent.Reader.readUntilDelimiter;
import static fixture.gcs.MultipartContent.Reader.skipUntilDelimiter;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;

public class MultipartContentTests extends ESTestCase {

    // produces content that does not contain boundary
    static BytesReference randomPartContent(int len, String boundary) {
        assert len > 0 && boundary.isEmpty() == false;
        var content = randomAlphanumericOfLength(len);
        var replacement = boundary.getBytes(UTF_8);
        replacement[0]++; // change single char to make it different from original
        content = content.replace(boundary, Arrays.toString(replacement));
        return new BytesArray(content);
    }

    static SequencedMap<String, String> randomHeaders() {
        final var headers = new LinkedHashMap<String, String>();
        final var numberOfHeaders = between(0, 10);
        for (var headerNum = 0; headerNum < numberOfHeaders; headerNum++) {
            headers.put("x-" + randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT), randomAlphanumericOfLength(between(1, 100)));
        }
        return headers;
    }

    static MultipartContent.Part randomPart(String boundary) {
        return new MultipartContent.Part(randomHeaders(), randomPartContent(between(1, 1024), boundary));
    }

    public void testEmptyPart() throws IOException {
        final var boundary = randomAlphanumericOfLength(between(1, 70));
        final var output = new ByteArrayOutputStream();
        final var writer = new MultipartContent.Writer(boundary, output);
        final var emptyPart = new MultipartContent.Part(new LinkedHashMap<>(), BytesArray.EMPTY);
        writer.write(emptyPart);
        writer.end();
        final var reader = MultipartContent.Reader.readStream(boundary, new ByteArrayInputStream(output.toByteArray()));
        assertEquals(emptyPart, reader.next());
    }

    public void testWriteAndReadParts() throws IOException {
        final var boundary = randomAlphanumericOfLength(between(1, 70));
        final var output = new ByteArrayOutputStream();
        final var writer = new MultipartContent.Writer(boundary, output);
        final var writeParts = IntStream.range(0, 100).mapToObj(i -> randomPart(boundary)).toList();
        for (var part : writeParts) {
            writer.write(part);
        }
        writer.end();

        final var input = new ByteArrayInputStream(output.toByteArray());
        final var reader = MultipartContent.Reader.readStream(boundary, input);
        final var readParts = new ArrayList<MultipartContent.Part>();
        while (reader.hasNext()) {
            readParts.add(reader.next());
        }
        for (int i = 0; i < writeParts.size(); i++) {
            assertEquals(writeParts.get(i), readParts.get(i));
        }
    }

    public void testReadUntilDelimiter() throws IOException {
        for (int run = 0; run < 100; run++) {
            var delimitedContent = DelimitedContent.randomContent();
            var inputStream = delimitedContent.toBytesReference().streamInput();
            var readBytes = readUntilDelimiter(inputStream, delimitedContent.delimiter);
            assertThat(readBytes, equalBytes(new BytesArray(delimitedContent.before)));
            var readRemaining = inputStream.readAllBytes();
            assertArrayEquals(delimitedContent.after, readRemaining);
        }
    }

    public void testSkipUntilDelimiter() throws IOException {
        for (int run = 0; run < 100; run++) {
            var delimitedContent = DelimitedContent.randomContent();
            var inputStream = delimitedContent.toBytesReference().streamInput();
            skipUntilDelimiter(inputStream, delimitedContent.delimiter);
            var readRemaining = inputStream.readAllBytes();
            assertArrayEquals(delimitedContent.after, readRemaining);
        }
    }

    record DelimitedContent(byte[] before, byte[] delimiter, byte[] after) {

        static DelimitedContent randomContent() {
            var before = randomAlphanumericOfLength(between(0, 1024 * 1024)).getBytes(UTF_8);
            var delimiter = randomByteArrayOfLength(between(1, 70));
            delimiter[0] = '\r'; // make it distinguishable from the initial bytes
            var after = randomAlphanumericOfLength(between(0, 1024 * 1024)).getBytes(UTF_8);
            return new DelimitedContent(before, delimiter, after);
        }

        BytesReference toBytesReference() {
            return CompositeBytesReference.of(new BytesArray(before), new BytesArray(delimiter), new BytesArray(after));
        }
    }

}
