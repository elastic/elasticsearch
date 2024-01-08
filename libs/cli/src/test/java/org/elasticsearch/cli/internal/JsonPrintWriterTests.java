/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.internal;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
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

            for (int i = 0; i < msg.length() - 10; i += 10) {
                String part = msg.substring(i, i + 10);
                if (randomBoolean()) writer.write(part);
                else writer.print(part);
            }

            String lastPart = msg.substring(msg.length() - msg.length() % 10, msg.length());
            if (randomBoolean()) {
                writer.println(lastPart);
            } else {
                writer.print(lastPart);
                writer.println();
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
        assertEquals(expected, outputStream.toString());
        outputStream.reset();
    }

    public void testToJsonHelper() {
        assertEquals(jsonMessage(null), JsonPrintWriter.toJson(null, FIXED_CLOCK.millis()));
        assertEquals(jsonMessage("some \\\"quoted\\\" text"), JsonPrintWriter.toJson("some \"quoted\" text", FIXED_CLOCK.millis()));
    }

    public void testIsJsonObject() {
        String ignore = randomUnicodeOfCodepointLengthBetween(10, 20);

        String prefix = WhitespaceGenerator.INSTANCE.ofCodePointsLength(random(), 0, 10);
        String suffix = WhitespaceGenerator.INSTANCE.ofCodePointsLength(random(), 0, 10);
        assertTrue(JsonPrintWriter.isJsonObject(prefix + "{" + ignore + "}" + suffix));

        String prefixOrSuffix = randomAsciiAlphanumOfLengthBetween(random(), 1, 5);
        assertFalse(JsonPrintWriter.isJsonObject(prefixOrSuffix + "{" + ignore + "}"));
        assertFalse(JsonPrintWriter.isJsonObject("{" + ignore + "}" + prefixOrSuffix));
    }

    private static String jsonMessage(String msg) {
        // timestamp according to fixed clock
        return "{\"@timestamp\":\"2024-01-08T11:06:54.000Z\", \"message\":\"" + msg + "\"}";
    }

    private static class WhitespaceGenerator extends CodepointSetGenerator {
        static final CodepointSetGenerator INSTANCE = new WhitespaceGenerator();

        WhitespaceGenerator() {
            super(new char[] { ' ', '\n', '\f', '\r', '\t' });
        }
    }
}
