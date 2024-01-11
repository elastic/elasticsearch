/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.internal;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomAsciiAlphanumOfLengthBetween;

public class JsonPrintWriterTests extends ESTestCase {
    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2024-01-08T11:06:54.0Z"), ZoneOffset.UTC);

    private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    public void testPrintLineBufferAsJsonAfterFlush() {
        try (PrintWriter writer = new JsonPrintWriter(outputStream, true, FIXED_CLOCK)) {
            String msg = randomAsciiAlphanumOfLengthBetween(random(), 10, 50);

            for (int i = 0; i < msg.length(); i += 10) {
                String part = msg.substring(i, Math.min(i + 10, msg.length()));
                if (randomBoolean()) writer.write(part);
                else writer.print(part);
            }

            writer.flush();
            assertWritten(jsonMessage(msg));
        }
    }

    public void testPrintLineBufferAsJsonAfterPrintln() {
        try (PrintWriter writer = new JsonPrintWriter(outputStream, true, FIXED_CLOCK)) {
            String msg = randomAsciiAlphanumOfLengthBetween(random(), 10, 50);

            int start = 0;
            while (true) {
                int end = start + 10;
                String part = msg.substring(start, Math.min(msg.length(), end));

                // complete last part using some form of println and break
                if (end >= msg.length()) {
                    if (randomBoolean()) {
                        writer.println(part);
                    } else {
                        writer.print(part);
                        writer.println();
                    }
                    break;
                }

                if (randomBoolean()) writer.write(part);
                else writer.print(part);
                start = end;
            }
            assertWritten(jsonMessage(msg) + System.lineSeparator());
        }
    }

    public void testPassThroughJson() {
        try (PrintWriter writer = new JsonPrintWriter(outputStream, true, FIXED_CLOCK)) {
            String looksLikeJson = "{" + randomUnicodeOfCodepointLengthBetween(10, 20) + "}";
            writer.write(looksLikeJson);
            writer.flush();
            assertWritten(looksLikeJson);

            writer.print(looksLikeJson);
            writer.flush();
            assertWritten(looksLikeJson);

            writer.println(looksLikeJson);
            assertWritten(looksLikeJson + System.lineSeparator());
        }
    }

    private void assertWritten(String expected) {
        assertEquals(expected, outputStream.toString(StandardCharsets.UTF_8));
        outputStream.reset();
    }

    public void testToJsonHelper() {
        assertEquals(jsonMessage(null), JsonPrintWriter.toJson(null, FIXED_CLOCK.instant()));
        assertEquals(jsonMessage("some \\\"quoted\\\" text"), JsonPrintWriter.toJson("some \"quoted\" text", FIXED_CLOCK.instant()));
    }

    private static String jsonMessage(String msg) {
        // timestamp according to fixed clock
        return "{\"@timestamp\":\"2024-01-08T11:06:54.000Z\", \"message\":\"" + msg + "\"}";
    }
}
