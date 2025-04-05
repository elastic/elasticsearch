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

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MultipartUploadTests extends ESTestCase {

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
