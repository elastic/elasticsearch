/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.IntStream;

public class ChunkedLoggingStreamTests extends ESTestCase {

    public static final Logger logger = LogManager.getLogger(ChunkedLoggingStreamTests.class);

    @TestLogging(reason = "testing logging", value = "org.elasticsearch.common.logging.ChunkedLoggingStreamTests:DEBUG")
    public void testLogMessageChunking() {
        // bugs are most likely near chunk boundaries, so test sizes that are within +/- 3 bytes of 0, 1, and 2 chunks:
        IntStream.rangeClosed(-3, 3)
            .flatMap(i -> IntStream.iterate(i, j -> j + ChunkedLoggingStream.CHUNK_SIZE).limit(3))
            .filter(i -> i >= 0)
            .sorted()
            .forEach(ChunkedLoggingStreamTests::runChunkingTest);
    }

    private static void runChunkingTest(int size) {
        final var bytes = new byte[size];
        Arrays.fill(bytes, (byte) '.');
        final var expectedBody = new String(bytes, StandardCharsets.ISO_8859_1);
        final var prefix = randomAlphaOfLength(10);
        final var level = randomFrom(Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR);
        final var referenceDocs = randomFrom(ReferenceDocs.values());
        assertEquals(expectedBody, ChunkedLoggingStreamTestUtils.getLoggedBody(logger, level, prefix, referenceDocs, () -> {
            try (var stream = new ChunkedLoggingStream(logger, level, prefix, referenceDocs)) {
                writeRandomly(stream, bytes);
            }
        }));
    }

    @TestLogging(reason = "testing logging", value = "org.elasticsearch.common.logging.ChunkedLoggingStreamTests:DEBUG")
    public void testEncodingRoundTrip() {
        final var bytes = randomByteArrayOfLength(between(0, 10000));
        final var level = randomFrom(Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR);
        final var referenceDocs = randomFrom(ReferenceDocs.values());
        assertEquals(
            new BytesArray(bytes),
            ChunkedLoggingStreamTestUtils.getDecodedLoggedBody(logger, level, "prefix", referenceDocs, () -> {
                try (var stream = ChunkedLoggingStream.create(logger, level, "prefix", referenceDocs)) {
                    writeRandomly(stream, bytes);
                }
            })
        );
    }

    private static void writeRandomly(OutputStream stream, byte[] bytes) throws IOException {
        for (var pos = 0; pos < bytes.length;) {
            if (randomBoolean()) {
                stream.write(bytes[pos++]);
            } else {
                var len = between(1, bytes.length - pos);
                stream.write(bytes, pos, len);
                pos += len;
            }
        }
    }

}
