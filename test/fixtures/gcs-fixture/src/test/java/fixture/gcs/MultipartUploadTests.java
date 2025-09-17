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
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MultipartUploadTests extends ESTestCase {

    // produces content that does not contain boundary
    static String randomPartContent(int len, String boundary) {
        assert len > 0 && boundary.isEmpty() == false;
        var content = randomAlphanumericOfLength(len);
        var replacement = boundary.getBytes(UTF_8);
        replacement[0]++; // change single char to make it different from original
        return content.replace(boundary, Arrays.toString(replacement));
    }

    public void testGenericMultipart() throws IOException {
        var boundary = randomAlphanumericOfLength(between(1, 70));
        var part1 = "plain text\nwith line break";
        var part2 = "";
        var part3 = randomPartContent(between(1, 1024), boundary);
        var strInput = """
            --$boundary\r
            \r
            \r
            $part1\r
            --$boundary\r
            X-Header: x-man\r
            \r
            $part2\r
            --$boundary\r
            Content-Type: application/octet-stream\r
            \r
            $part3\r
            --$boundary--""".replace("$boundary", boundary).replace("$part1", part1).replace("$part2", part2).replace("$part3", part3);

        var reader = new MultipartUpload.MultipartContentReader(boundary, new ByteArrayStreamInput(strInput.getBytes()));
        assertEquals(part1, reader.next().utf8ToString());
        assertEquals(part2, reader.next().utf8ToString());
        assertEquals(part3, reader.next().utf8ToString());
        assertFalse(reader.hasNext());
    }

    public void testReadUntilDelimiter() throws IOException {
        for (int run = 0; run < 100; run++) {
            var delimitedContent = DelimitedContent.randomContent();
            var inputStream = delimitedContent.toBytesReference().streamInput();
            var readBytes = MultipartUpload.readUntilDelimiter(inputStream, delimitedContent.delimiter);
            assertEquals(new BytesArray(delimitedContent.before), readBytes);
            var readRemaining = inputStream.readAllBytes();
            assertArrayEquals(delimitedContent.after, readRemaining);
        }
    }

    public void testSkipUntilDelimiter() throws IOException {
        for (int run = 0; run < 100; run++) {
            var delimitedContent = DelimitedContent.randomContent();
            var inputStream = delimitedContent.toBytesReference().streamInput();
            MultipartUpload.skipUntilDelimiter(inputStream, delimitedContent.delimiter);
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
