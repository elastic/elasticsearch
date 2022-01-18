/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.logging.LoggingOutputStream.DEFAULT_BUFFER_LENGTH;
import static org.elasticsearch.common.logging.LoggingOutputStream.MAX_BUFFER_LENGTH;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class LoggingOutputStreamTests extends ESTestCase {

    class TestLoggingOutputStream extends LoggingOutputStream {
        List<String> lines = new ArrayList<>();

        TestLoggingOutputStream() {
            super(null, null);
        }

        @Override
        void log(String msg) {
            lines.add(msg);
        }
    }

    TestLoggingOutputStream loggingStream;
    PrintStream printStream;

    @Before
    public void createStream() {
        loggingStream = new TestLoggingOutputStream();
        printStream = new PrintStream(loggingStream, false, StandardCharsets.UTF_8);
    }

    public void testEmptyLineUnix() {
        printStream.print("\n");
        assertTrue(loggingStream.lines.isEmpty());
        printStream.flush();
        assertTrue(loggingStream.lines.isEmpty());
    }

    public void testEmptyLineWindows() {
        printStream.print("\r\n");
        assertTrue(loggingStream.lines.isEmpty());
        printStream.flush();
        assertTrue(loggingStream.lines.isEmpty());
    }

    public void testNull() {
        printStream.write(0);
        printStream.flush();
        assertTrue(loggingStream.lines.isEmpty());
    }

    // this test explicitly outputs the newlines instead of relying on println, to always test the unix behavior
    public void testFlushOnUnixNewline() {
        printStream.print("hello\n");
        printStream.print("\n"); // newline by itself does not show up
        printStream.print("world\n");
        assertThat(loggingStream.lines, contains("hello", "world"));
    }

    // this test explicitly outputs the newlines instead of relying on println, to always test the windows behavior
    public void testFlushOnWindowsNewline() {
        printStream.print("hello\r\n");
        printStream.print("\r\n"); // newline by itself does not show up
        printStream.print("world\r\n");
        assertThat(loggingStream.lines, contains("hello", "world"));
    }

    public void testBufferExtension() {
        String longStr = randomAlphaOfLength(DEFAULT_BUFFER_LENGTH);
        String extraLongStr = randomAlphaOfLength(DEFAULT_BUFFER_LENGTH + 1);
        printStream.println(longStr);
        assertThat(loggingStream.threadLocal.get().bytes.length, equalTo(DEFAULT_BUFFER_LENGTH));
        printStream.println(extraLongStr);
        assertThat(loggingStream.lines, contains(longStr, extraLongStr));
        assertThat(loggingStream.threadLocal.get().bytes.length, equalTo(DEFAULT_BUFFER_LENGTH));
    }

    public void testMaxBuffer() {
        String longStr = randomAlphaOfLength(MAX_BUFFER_LENGTH);
        String extraLongStr = longStr + "OVERFLOW";
        printStream.println(longStr);
        printStream.println(extraLongStr);
        assertThat(loggingStream.lines, contains(longStr, longStr, "OVERFLOW"));
    }

    public void testClosed() {
        loggingStream.close();
        IOException e = expectThrows(IOException.class, () -> loggingStream.write('a'));
        assertThat(e.getMessage(), containsString("buffer closed"));
    }

    public void testThreadIsolation() throws Exception {
        printStream.print("from thread 1");
        Thread thread2 = new Thread(() -> { printStream.println("from thread 2"); });
        thread2.start();
        thread2.join();
        printStream.flush();
        assertThat(loggingStream.lines, contains("from thread 2", "from thread 1"));
    }
}
