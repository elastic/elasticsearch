/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TerminalTests extends ESTestCase {

    public void testSystemTerminalIfRedirected() {
        // Expect system terminal if redirected for tests.
        // To force new behavior in JDK 22 this should run without security manager.
        // Otherwise, JDK 22 doesn't provide a console if redirected.
        assertEquals(Terminal.SystemTerminal.class, Terminal.DEFAULT.getClass());
    }

    public void testTerminalAsLineOutputStream() throws IOException {
        PrintWriter stdOut = mock("stdOut");
        PrintWriter stdErr = mock("stdErr");

        OutputStream out = new Terminal(mock("reader"), stdOut, stdErr) {
        }.asLineOutputStream(StandardCharsets.UTF_8);

        out.write("123".getBytes(StandardCharsets.UTF_8));
        out.write("456".getBytes(StandardCharsets.UTF_8));
        out.write("789\r\n".getBytes(StandardCharsets.UTF_8)); // CR is removed as well

        verify(stdOut).println(eq((CharSequence) "123456789"));
        verify(stdOut).flush();
        verifyNoMoreInteractions(stdOut, stdErr);

        out.write("\n".getBytes(StandardCharsets.UTF_8));
        verify(stdOut).println(eq((CharSequence) ""));
        verify(stdOut, times(2)).flush();
        verifyNoMoreInteractions(stdOut, stdErr);

        out.write("a".getBytes(StandardCharsets.UTF_8));
        out.flush();
        verify(stdOut).print(eq((CharSequence) "a"));
        verify(stdOut, times(3)).flush();

        out.flush();
        verifyNoMoreInteractions(stdOut, stdErr);
    }
}
