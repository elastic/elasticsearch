/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.IntStream;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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
        assertEquals(expectedBody, getLoggedBody(logger, level, prefix, referenceDocs, () -> {
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
        assertEquals(new BytesArray(bytes), getDecodedLoggedBody(logger, level, "prefix", referenceDocs, () -> {
            try (var stream = ChunkedLoggingStream.create(logger, level, "prefix", referenceDocs)) {
                writeRandomly(stream, bytes);
            }
        }));
    }

    private static String getLoggedBody(
        Logger captureLogger,
        final Level level,
        String prefix,
        final ReferenceDocs referenceDocs,
        CheckedRunnable<Exception> runnable
    ) {
        class ChunkReadingAppender extends AbstractAppender {
            final StringBuilder encodedResponseBuilder = new StringBuilder();
            int chunks;
            boolean seenTotal;

            ChunkReadingAppender() {
                super("mock", null, null, false, Property.EMPTY_ARRAY);
            }

            @Override
            public void append(LogEvent event) {
                if (event.getLevel() != level) {
                    return;
                }
                if (event.getLoggerName().equals(captureLogger.getName()) == false) {
                    return;
                }
                assertFalse(seenTotal);
                final var message = event.getMessage().getFormattedMessage();
                final var onePartPrefix = prefix + " (gzip compressed and base64-encoded; for details see " + referenceDocs + "): ";
                final var partPrefix = prefix + " [part " + (chunks + 1) + "]: ";
                if (message.startsWith(partPrefix)) {
                    chunks += 1;
                    final var chunk = message.substring(partPrefix.length());
                    assertThat(chunk.length(), lessThanOrEqualTo(ChunkedLoggingStream.CHUNK_SIZE));
                    encodedResponseBuilder.append(chunk);
                } else if (message.startsWith(onePartPrefix)) {
                    assertEquals(0, chunks);
                    chunks += 1;
                    final var chunk = message.substring(onePartPrefix.length());
                    assertThat(chunk.length(), lessThanOrEqualTo(ChunkedLoggingStream.CHUNK_SIZE));
                    encodedResponseBuilder.append(chunk);
                    seenTotal = true;
                } else {
                    assertEquals(
                        prefix
                            + " (gzip compressed, base64-encoded, and split into "
                            + chunks
                            + " parts on preceding log lines; for details see "
                            + referenceDocs
                            + ")",
                        message
                    );
                    assertThat(chunks, greaterThan(1));
                    seenTotal = true;
                }
            }
        }

        final var appender = new ChunkReadingAppender();
        try {
            appender.start();
            Loggers.addAppender(captureLogger, appender);
            runnable.run();
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        } finally {
            Loggers.removeAppender(captureLogger, appender);
            appender.stop();
        }

        assertThat(appender.chunks, greaterThan(0));
        assertTrue(appender.seenTotal);

        return appender.encodedResponseBuilder.toString();
    }

    /**
     * Test utility function which captures the logged output from a {@link ChunkedLoggingStream}, combines the chunks, Base64-decodes it
     * and Gzip-decompresses it to retrieve the original data.
     *
     * @param captureLogger The logger whose output should be captured.
     * @param level         The log level for the data.
     * @param prefix        The prefix used by the logging stream.
     * @param referenceDocs A link to the reference docs about the output.
     * @param runnable      The action which emits the logs.
     * @return              A {@link BytesReference} containing the captured data.
     */
    public static BytesReference getDecodedLoggedBody(
        Logger captureLogger,
        Level level,
        String prefix,
        ReferenceDocs referenceDocs,
        CheckedRunnable<Exception> runnable
    ) {
        final var loggedBody = getLoggedBody(captureLogger, level, prefix, referenceDocs, runnable);

        try (
            var bytesStreamOutput = new BytesStreamOutput();
            var byteArrayInputStream = new ByteArrayInputStream(Base64.getDecoder().decode(loggedBody));
            var gzipInputStream = new GZIPInputStream(byteArrayInputStream)
        ) {
            Streams.copy(gzipInputStream, bytesStreamOutput);
            return bytesStreamOutput.bytes();
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
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
