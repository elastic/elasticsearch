/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli.terminal.internal;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.startsWith;

public class TerminalPrintStreamTests extends ESTestCase {

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { false }, new Object[] { true });
    }

    private final boolean isError;

    private MockTerminal mockTerminal;
    private TerminalPrintStream stream;

    public TerminalPrintStreamTests(@Name("isError") boolean isError) {
        this.isError = isError;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockTerminal = MockTerminal.create();
        stream = new TerminalPrintStream(mockTerminal, isError);
    }

    private String activeOutput() {
        return isError ? mockTerminal.getErrorOutput() : mockTerminal.getOutput();
    }

    private String inactiveOutput() {
        return isError ? mockTerminal.getOutput() : mockTerminal.getErrorOutput();
    }

    private String[] outputLines() {
        String output = activeOutput();
        return output.isEmpty() ? new String[0] : output.split("\\R");
    }

    private void assertInactiveOutputIsEmpty() {
        assertEquals("inactive stream should not receive output", "", inactiveOutput());
    }

    public void testPrintlnWritesPlainTextLine() {
        stream.println("WARNING: Using incubator modules");

        assertArrayEquals(new String[] { "WARNING: Using incubator modules" }, outputLines());
        assertInactiveOutputIsEmpty();
    }

    public void testMultiplePrintlnCalls() {
        stream.println("line one");
        stream.println("line two");

        String[] lines = outputLines();
        assertEquals(2, lines.length);
        assertEquals("line one", lines[0]);
        assertEquals("line two", lines[1]);
        assertInactiveOutputIsEmpty();
    }

    public void testRawBytesWithNewline() {
        stream.write("raw line\n".getBytes(UTF_8), 0, "raw line\n".length());

        assertArrayEquals(new String[] { "raw line" }, outputLines());
        assertInactiveOutputIsEmpty();
    }

    public void testRawBytesMultipleLines() {
        byte[] data = "line one\nline two\n".getBytes(UTF_8);
        stream.write(data, 0, data.length);

        String[] lines = outputLines();
        assertEquals(2, lines.length);
        assertEquals("line one", lines[0]);
        assertEquals("line two", lines[1]);
        assertInactiveOutputIsEmpty();
    }

    public void testCarriageReturnStripped() {
        byte[] data = "windows line\r\n".getBytes(UTF_8);
        stream.write(data, 0, data.length);

        assertArrayEquals(new String[] { "windows line" }, outputLines());
        assertInactiveOutputIsEmpty();
    }

    public void testFlushWithoutNewlineEmitsLine() {
        byte[] data = "partial".getBytes(UTF_8);
        stream.write(data, 0, data.length);
        stream.flush();

        assertArrayEquals(new String[] { "partial" }, outputLines());
        assertInactiveOutputIsEmpty();
    }

    public void testEmptyLineIsDropped() {
        stream.write('\n');

        String output = activeOutput();
        assertEquals("bare newline should produce no output", "", output);
        assertInactiveOutputIsEmpty();
    }

    public void testByteAtATimeWriting() {
        byte[] data = "byte by byte\n".getBytes(UTF_8);
        for (byte b : data) {
            stream.write(b);
        }

        assertArrayEquals(new String[] { "byte by byte" }, outputLines());
        assertInactiveOutputIsEmpty();
    }

    public void testJsonFromPreparerWritesAsPlainText() {
        String preparerOutput = "{\"@timestamp\":\"2024-01-01T00:00:00.000Z\",\"log.level\":\"INFO\","
            + "\"log.logger\":\"serverless.cli\",\"message\":\"Starting Serverless Elasticsearch...\"}";
        stream.println(preparerOutput);

        assertArrayEquals(new String[] { preparerOutput }, outputLines());
        assertInactiveOutputIsEmpty();
    }

    public void testRawBytesJsonPassThroughAsPlainText() {
        String original = "{\"@timestamp\":\"2024-01-01T00:00:00.000Z\",\"message\":\"from ES\"}";
        byte[] data = (original + "\n").getBytes(UTF_8);
        stream.write(data, 0, data.length);

        assertArrayEquals(new String[] { original }, outputLines());
        assertInactiveOutputIsEmpty();
    }

    public void testPartialWritesThenPrintln() {
        byte[] partial = "start ".getBytes(UTF_8);
        stream.write(partial, 0, partial.length);
        stream.println("end");

        assertArrayEquals(new String[] { "start end" }, outputLines());
        assertInactiveOutputIsEmpty();
    }

    private static Runnable repeat(CyclicBarrier barrier, int iterations, Consumer<String> consumer) {
        return () -> {
            try {
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    consumer.accept("msg-" + i);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public void testConcurrentWritersDoNotCorruptLines() throws Exception {
        int iterations = 500;
        CyclicBarrier barrier = new CyclicBarrier(4);

        ExecutorService executor = Executors.newFixedThreadPool(barrier.getParties());
        executor.execute(repeat(barrier, iterations, msg -> stream.write((msg + "\n").getBytes(UTF_8), 0, msg.length() + 1)));
        executor.execute(repeat(barrier, iterations, msg -> stream.write((msg + "\n").getBytes(UTF_8), 0, msg.length() + 1)));
        executor.execute(repeat(barrier, iterations, stream::println));
        executor.execute(repeat(barrier, iterations, stream::println));
        executor.shutdown();

        assertTrue(executor.awaitTermination(30, SECONDS));

        String[] lines = outputLines();
        assertEquals(iterations * barrier.getParties(), lines.length);
        for (String line : lines) {
            assertThat(line, startsWith("msg-"));
        }
        assertInactiveOutputIsEmpty();
    }

}
