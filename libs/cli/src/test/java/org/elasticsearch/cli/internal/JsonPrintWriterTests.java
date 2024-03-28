/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.internal;

import joptsimple.OptionParser;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomAsciiAlphanumOfLengthBetween;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;

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

    public void testPrintStacktrace() throws IOException {
        Exception exception = new RuntimeException("Something went wrong!");
        try (JsonPrintWriter writer = jsonPrintWriter()) {
            writer.println(exception); // should not require flush

            String jsonString = outputStream.toString(StandardCharsets.UTF_8);
            assertThat(jsonString, endsWith(System.lineSeparator()));

            Map<String, Object> fields = createParser(JsonXContent.jsonXContent, jsonString).map(); // un-quoted fields
            assertThat(fields.get("message"), is("Something went wrong!"));
            assertThat(fields.get("error.type"), is("java.lang.RuntimeException"));
            assertThat(fields, hasKey("error.stack_trace"));

            String[] lines = fields.get("error.stack_trace").toString().split(System.lineSeparator());
            assertThat(lines[0], is("java.lang.RuntimeException: Something went wrong!"));
            assertThat(lines[1], matchesRegex("\tat [a-zA-Z.]+\\.testPrintStacktrace\\(JsonPrintWriterTests.java:\\d+\\)"));
            assertThat(lines[lines.length - 1], matchesRegex("\tat java.base/java.lang.Thread.run\\(Thread.java:\\d+\\)"));
        }
    }

    public void testPrintOptionParserHelp() throws IOException {
        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("h", "help"), "Show help").forHelp();

        try (JsonPrintWriter writer = jsonPrintWriter()) {
            parser.printHelpOn(writer); // should not require flush

            String jsonString = outputStream.toString(StandardCharsets.UTF_8);
            Map<String, Object> fields = createParser(JsonXContent.jsonXContent, jsonString).map(); // un-quoted fields

            assertThat(fields, hasKey("message"));
            String message = fields.get("message").toString();

            assertThat(jsonString, endsWith(System.lineSeparator())); // trailing newline
            assertThat(message, matchesRegex("Option +Description *\n-+ +-+ *\n-h, --help +Show help *")); // no trailing newline
        }
    }

    public void testStripSuffix() {
        StringBuilder builder = new StringBuilder("Hello World");
        assertTrue(JsonPrintWriter.stripSuffix(builder, "World"));
        assertEquals("Hello ", builder.toString());

        builder = new StringBuilder("Hello World");
        assertTrue(JsonPrintWriter.stripSuffix(builder, "Hello World"));
        assertEquals("", builder.toString());

        builder = new StringBuilder("Hello");
        assertFalse(JsonPrintWriter.stripSuffix(builder, "Hello World"));
        assertEquals("Hello", builder.toString());
    }

    /** repeat testCase multiple times to verify reset of internal buffer. */
    private void runRepeatedly(Consumer<PrintWriter> testCase) {
        try (PrintWriter writer = jsonPrintWriter()) {
            for (int runs = 2; runs > 0; runs--) {
                testCase.accept(writer);
            }
            writer.flush();
            assertWritten("");
        }
    }

    private JsonPrintWriter jsonPrintWriter() {
        Map<String, String> fields = Map.of("field1", "value1", "field2", "value2");
        return new JsonPrintWriter(new TreeMap<>(fields), outputStream, true, FIXED_CLOCK);
    }

    private void assertWritten(String expected) {
        assertEquals(expected, outputStream.toString(StandardCharsets.UTF_8));
        outputStream.reset();
    }

    private static String jsonMessage(String msg) {
        // timestamp according to fixed clock
        return "{\"field1\":\"value1\",\"field2\":\"value2\",\"@timestamp\":\"2024-01-08T11:06:54.000Z\", \"message\":\"" + msg + "\"}";
    }
}
