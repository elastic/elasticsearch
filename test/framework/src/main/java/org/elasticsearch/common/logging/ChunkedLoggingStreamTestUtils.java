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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.util.Base64;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Utility for capturing and decoding the data logged by a {@link ChunkedLoggingStream}.
 */
public class ChunkedLoggingStreamTestUtils {

    private ChunkedLoggingStreamTestUtils() {/* no instances */}

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
            return ESTestCase.fail(e);
        }
    }

    static String getLoggedBody(
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
                Assert.assertFalse(seenTotal);
                final var message = event.getMessage().getFormattedMessage();
                final var onePartPrefix = prefix + " (gzip compressed and base64-encoded; for details see " + referenceDocs + "): ";
                final var partPrefix = prefix + " [part " + (chunks + 1) + "]: ";
                if (message.startsWith(partPrefix)) {
                    chunks += 1;
                    final var chunk = message.substring(partPrefix.length());
                    ESTestCase.assertThat(chunk.length(), lessThanOrEqualTo(ChunkedLoggingStream.CHUNK_SIZE));
                    encodedResponseBuilder.append(chunk);
                } else if (message.startsWith(onePartPrefix)) {
                    Assert.assertEquals(0, chunks);
                    chunks += 1;
                    final var chunk = message.substring(onePartPrefix.length());
                    ESTestCase.assertThat(chunk.length(), lessThanOrEqualTo(ChunkedLoggingStream.CHUNK_SIZE));
                    encodedResponseBuilder.append(chunk);
                    seenTotal = true;
                } else {
                    Assert.assertEquals(
                        prefix
                            + " (gzip compressed, base64-encoded, and split into "
                            + chunks
                            + " parts on preceding log lines; for details see "
                            + referenceDocs
                            + ")",
                        message
                    );
                    ESTestCase.assertThat(chunks, greaterThan(1));
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
            ESTestCase.fail(e);
        } finally {
            Loggers.removeAppender(captureLogger, appender);
            appender.stop();
        }

        ESTestCase.assertThat(appender.chunks, greaterThan(0));
        Assert.assertTrue(appender.seenTotal);

        return appender.encodedResponseBuilder.toString();
    }
}
