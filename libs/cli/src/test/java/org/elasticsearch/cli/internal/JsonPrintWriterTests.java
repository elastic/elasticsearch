/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.internal;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.function.Consumer;

import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomAsciiAlphanumOfLengthBetween;

public class JsonPrintWriterTests extends ESTestCase {
    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2024-01-08T11:06:54.0Z"), ZoneOffset.UTC);

    private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    public void testPrintLineBufferAsJsonAfterFlush() {
        runRepeatedly(writer -> {
            String msg = randomAsciiAlphanumOfLengthBetween(random(), 10, 50);
            for (int start = 0; start < msg.length(); start += 10) {
                String part = msg.substring(start, Math.min(start + 10, msg.length()));
                writer.print(part);
            }

            writer.flush();
            assertWritten(jsonMessage(msg));
        });
    }

    public void testPrintLineBufferAsJsonAfterPrintln() {
        runRepeatedly(writer -> {
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
        });
    }

    public void testWriteEscapeJson() {
        runRepeatedly(writer -> {
            writer.write("some \"quoted\" text");
            writer.flush();
            assertWritten(jsonMessage("some \\\"quoted\\\" text"));
        });
    }

    public void testPrintlnJsonPassThrough() {
        // we check for json when using println(String), println(Object), println(char[])
        runRepeatedly(writer -> {
            // repeat multiple times to verify reset of internal buffer
            for (int i = 0; i < 3; i++) {
                String looksLikeJson = "{" + randomUnicodeOfCodepointLengthBetween(10, 20) + "}";

                writer.println(looksLikeJson);
                assertWritten(looksLikeJson + System.lineSeparator());

                writer.println((CharSequence) looksLikeJson);
                assertWritten(looksLikeJson + System.lineSeparator());

                writer.println(looksLikeJson.toCharArray());
                assertWritten(looksLikeJson + System.lineSeparator());
            }
        });
    }

    public void testPrintAPI() {
        // anything written using the print (not println) API is buffered & converted to JSON
        runRepeatedly(writer -> {
            StringBuilder expected = new StringBuilder();

            // if checked, this would look like JSON
            assert JsonPrintWriter.isJsonObject("{ not considered JSON");
            String str = randomBoolean() ? "{ not considered JSON" : "string";

            writer.print(str);
            writer.print((Object) str);
            writer.print(str.toCharArray());
            expected.append(str).append(str).append(str);

            writer.print(str.charAt(0));
            expected.append(str.charAt(0));

            boolean bool = randomBoolean();
            writer.print(bool);
            expected.append(bool);

            int number = randomInt();
            writer.print(number);
            writer.print((long) number);
            writer.print((float) number);
            writer.print((double) number);
            expected.append(number).append(number).append((float) number).append((double) number);

            writer.flush();
            assertWritten(jsonMessage(expected.toString()));
        });
    }

    public void testWriteAPI() {
        // anything written using the write API is buffered & converted to JSON
        runRepeatedly(writer -> {
            StringBuilder expected = new StringBuilder();

            // if checked, this would look like JSON
            assert JsonPrintWriter.isJsonObject("{ not considered JSON");
            String str = randomBoolean() ? "{notConsideredJson" : "string";

            writer.write(str.toCharArray());
            writer.write(str.toCharArray(), 0, 3);
            expected.append(str).append(str, 0, 3);

            writer.write(str);
            writer.write(str, 0, 3);
            expected.append(str).append(str, 0, 3);

            writer.write(str.charAt(0));
            expected.append(str.charAt(0));

            writer.flush();
            assertWritten(jsonMessage(expected.toString()));
        });
    }

    public void testAppendAPI() {
        // anything written using the append API is buffered & converted to JSON
        runRepeatedly(writer -> {
            StringBuilder expected = new StringBuilder();

            // if checked, this would look like JSON
            assert JsonPrintWriter.isJsonObject("{ not considered JSON");
            String str = randomBoolean() ? "{notConsideredJson" : "string";

            writer.append(str);
            writer.append(str, 0, 3);
            expected.append(str).append(str, 0, 3);

            writer.append(str.charAt(0));
            expected.append(str.charAt(0));

            writer.flush();
            assertWritten(jsonMessage(expected.toString()));
        });
    }

    @SuppressForbidden(reason = "testing behavior without locale")
    public void testFormatAPI() {
        // anything written using the format API is buffered & converted to JSON
        runRepeatedly(writer -> {
            StringBuilder expected = new StringBuilder();

            // if checked, this would look like JSON
            assert JsonPrintWriter.isJsonObject("{ not considered JSON");
            String str = randomBoolean() ? "{notConsideredJson" : "string";

            writer.format("%s:%s", str, str);
            expected.append(str).append(':').append(str);

            writer.format(Locale.ROOT, "%s:%s", str, str);
            expected.append(str).append(':').append(str);

            writer.flush();
            assertWritten(jsonMessage(expected.toString()));
        });
    }

    @SuppressForbidden(reason = "testing behavior without locale")
    public void testPrintfAPI() {
        // anything written using the format API is buffered & converted to JSON
        runRepeatedly(writer -> {
            StringBuilder expected = new StringBuilder();

            // if checked, this would look like JSON
            assert JsonPrintWriter.isJsonObject("{ not considered JSON");
            String str = randomBoolean() ? "{notConsideredJson" : "string";

            writer.printf("%s:%s", str, str);
            expected.append(str).append(':').append(str);

            writer.printf(Locale.ROOT, "%s:%s", str, str);
            expected.append(str).append(':').append(str);

            writer.flush();
            assertWritten(jsonMessage(expected.toString()));
        });
    }

    /** repeat testCase multiple times to verify reset of internal buffer. */
    private void runRepeatedly(Consumer<PrintWriter> testCase) {
        try (PrintWriter writer = new JsonPrintWriter(outputStream, true, FIXED_CLOCK)) {
            for (int runs = 2; runs > 0; runs--) {
                testCase.accept(writer);
            }
            writer.flush();
            assertWritten("");
        }
    }

    private void assertWritten(String expected) {
        assertEquals(expected, outputStream.toString(StandardCharsets.UTF_8));
        outputStream.reset();
    }

    private static String jsonMessage(String msg) {
        // timestamp according to fixed clock
        return "{\"@timestamp\":\"2024-01-08T11:06:54.000Z\", \"message\":\"" + msg + "\"}";
    }
}
