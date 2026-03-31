/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.launcher;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.launcher.CliToolLauncher.createShutdownHook;
import static org.elasticsearch.launcher.CliToolLauncher.getToolName;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;

@SuppressForbidden(reason = "uses System.out and System.err")
public class CliToolLauncherTests extends ESTestCase {

    public void testCliNameSysprop() {
        assertThat(getToolName(Map.of("cli.name", "mycli")), equalTo("mycli"));
    }

    public void testScriptNameSysprop() {
        var sysprops = Map.of("cli.name", "", "cli.script", "/foo/bar/elasticsearch-mycli", "os.name", "Linux");
        assertThat(getToolName(sysprops), equalTo("mycli"));
    }

    public void testScriptNameSyspropWindows() {
        var sysprops = Map.of("cli.name", "", "cli.script", "C:\\foo\\bar\\elasticsearch-mycli.bat", "os.name", "Windows XP");
        assertThat(getToolName(sysprops), equalTo("mycli"));
        sysprops = Map.of("cli.name", "", "cli.script", "C:\\foo\\bar\\elasticsearch-mycli", "os.name", "Windows XP");
        assertThat(getToolName(sysprops), equalTo("mycli"));
    }

    public void testShutdownHook() {
        MockTerminal terminal = MockTerminal.create();
        AtomicBoolean closeCalled = new AtomicBoolean();
        Closeable toClose = () -> { closeCalled.set(true); };
        Thread hook = createShutdownHook(terminal, toClose);
        hook.run();
        assertThat(closeCalled.get(), is(true));
        assertThat(terminal.getErrorOutput(), emptyString());
    }

    public void testShutdownHookError() {
        MockTerminal terminal = MockTerminal.create();
        Closeable toClose = () -> { throw new IOException("something bad happened"); };
        Thread hook = createShutdownHook(terminal, toClose);
        hook.run();
        assertThat(terminal.getErrorOutput(), containsString("something bad happened"));
        // ensure that we dump the stack trace too
        assertThat(terminal.getErrorOutput(), containsString("at org.elasticsearch.launcher.CliToolLauncherTests.lambda"));
    }

    public void testConsoleOutputWithoutRedirectFlag() throws Exception {
        Path esHome = createTempDir();
        ByteArrayOutputStream stdoutCapture = new ByteArrayOutputStream();
        ByteArrayOutputStream stderrCapture = new ByteArrayOutputStream();
        PrintStream savedOut = System.out;
        PrintStream savedErr = System.err;
        String savedEsPathHome = System.getProperty("es.path.home");
        String savedCliName = System.getProperty("cli.name");
        String savedCliLibs = System.getProperty("cli.libs");
        try {
            System.setOut(new PrintStream(stdoutCapture, true, StandardCharsets.UTF_8));
            System.setErr(new PrintStream(stderrCapture, true, StandardCharsets.UTF_8));
            System.setProperty("es.path.home", esHome.toString());
            System.setProperty("cli.name", "redirect-test");
            System.setProperty("cli.libs", "");
            CliToolLauncher.main(new String[0]);
        } finally {
            System.setOut(savedOut);
            System.setErr(savedErr);
            restoreOrClear("es.path.home", savedEsPathHome);
            restoreOrClear("cli.name", savedCliName);
            restoreOrClear("cli.libs", savedCliLibs);
        }
        // Without redirect, getOutputStream() returns current System.out (our capture), so sentinel bytes appear on stdout
        String stdout = stdoutCapture.toString(StandardCharsets.UTF_8);
        assertThat(stdout, containsString(new String(RedirectTestCommand.SENTINEL_BYTES, StandardCharsets.UTF_8)));
    }

    public void testWithRedirectFlagGetOutputStreamDirectsToStdout() throws Exception {
        Path esHome = createTempDir();
        ByteArrayOutputStream stdoutCapture = new ByteArrayOutputStream();
        ByteArrayOutputStream stderrCapture = new ByteArrayOutputStream();
        PrintStream savedOut = System.out;
        PrintStream savedErr = System.err;
        String savedEsPathHome = System.getProperty("es.path.home");
        String savedCliName = System.getProperty("cli.name");
        String savedCliLibs = System.getProperty("cli.libs");
        String savedRedirect = System.getProperty("cli.redirectStdoutToStderr");
        try {
            System.setOut(new PrintStream(stdoutCapture, true, StandardCharsets.UTF_8));
            System.setErr(new PrintStream(stderrCapture, true, StandardCharsets.UTF_8));
            System.setProperty("es.path.home", esHome.toString());
            System.setProperty("cli.name", "redirect-test");
            System.setProperty("cli.libs", "");
            System.setProperty("cli.redirectStdoutToStderr", "true");
            CliToolLauncher.main(new String[0]);
        } finally {
            System.setOut(savedOut);
            System.setErr(savedErr);
            restoreOrClear("es.path.home", savedEsPathHome);
            restoreOrClear("cli.name", savedCliName);
            restoreOrClear("cli.libs", savedCliLibs);
            restoreOrClear("cli.redirectStdoutToStderr", savedRedirect);
        }
        byte[] stdoutBytes = stdoutCapture.toByteArray();
        assertArrayEquals(RedirectTestCommand.SENTINEL_BYTES, stdoutBytes);
    }

    public void testWithRedirectFlagUserOutputGoesToStderr() throws Exception {
        Path esHome = createTempDir();
        ByteArrayOutputStream stdoutCapture = new ByteArrayOutputStream();
        ByteArrayOutputStream stderrCapture = new ByteArrayOutputStream();
        PrintStream savedOut = System.out;
        PrintStream savedErr = System.err;
        String savedEsPathHome = System.getProperty("es.path.home");
        String savedCliName = System.getProperty("cli.name");
        String savedCliLibs = System.getProperty("cli.libs");
        String savedRedirect = System.getProperty("cli.redirectStdoutToStderr");
        try {
            System.setOut(new PrintStream(stdoutCapture, true, StandardCharsets.UTF_8));
            System.setErr(new PrintStream(stderrCapture, true, StandardCharsets.UTF_8));
            System.setProperty("es.path.home", esHome.toString());
            System.setProperty("cli.name", "redirect-test");
            System.setProperty("cli.libs", "");
            System.setProperty("cli.redirectStdoutToStderr", "true");
            CliToolLauncher.main(new String[0]);
        } finally {
            System.setOut(savedOut);
            System.setErr(savedErr);
            restoreOrClear("es.path.home", savedEsPathHome);
            restoreOrClear("cli.name", savedCliName);
            restoreOrClear("cli.libs", savedCliLibs);
            restoreOrClear("cli.redirectStdoutToStderr", savedRedirect);
        }
        String stderr = stderrCapture.toString(StandardCharsets.UTF_8);
        assertThat(stderr, containsString(RedirectTestCommand.USER_OUTPUT));
    }

    public void testWithRedirectFlagStdoutOutputInStdoutMode() throws Exception {
        Path esHome = createTempDir();
        ByteArrayOutputStream stdoutCapture = new ByteArrayOutputStream();
        ByteArrayOutputStream stderrCapture = new ByteArrayOutputStream();
        PrintStream savedOut = System.out;
        PrintStream savedErr = System.err;
        String savedEsPathHome = System.getProperty("es.path.home");
        String savedCliName = System.getProperty("cli.name");
        String savedCliLibs = System.getProperty("cli.libs");
        String savedRedirect = System.getProperty("cli.redirectStdoutToStderr");
        try {
            System.setOut(new PrintStream(stdoutCapture, true, StandardCharsets.UTF_8));
            System.setErr(new PrintStream(stderrCapture, true, StandardCharsets.UTF_8));
            System.setProperty("es.path.home", esHome.toString());
            System.setProperty("cli.name", "redirect-test");
            System.setProperty("cli.libs", "");
            System.setProperty("cli.redirectStdoutToStderr", "true");
            CliToolLauncher.main(new String[0]);
        } finally {
            System.setOut(savedOut);
            System.setErr(savedErr);
            restoreOrClear("es.path.home", savedEsPathHome);
            restoreOrClear("cli.name", savedCliName);
            restoreOrClear("cli.libs", savedCliLibs);
            restoreOrClear("cli.redirectStdoutToStderr", savedRedirect);
        }
        byte[] raw = stderrCapture.toByteArray();
        byte[] userOutputBytes = RedirectTestCommand.USER_OUTPUT.getBytes(StandardCharsets.UTF_8);
        int idx = indexOf(raw, userOutputBytes);
        assertThat("user output should appear in muxed stream", idx >= 0, is(true));
        assertThat("stdout-destined output should be in STDOUT_MODE", activeModeAt(raw, idx), equalTo(OutputStreamMux.STDOUT_MODE));
    }

    public void testWithRedirectFlagErrorOutputInStderrMode() throws Exception {
        Path esHome = createTempDir();
        ByteArrayOutputStream stdoutCapture = new ByteArrayOutputStream();
        ByteArrayOutputStream stderrCapture = new ByteArrayOutputStream();
        PrintStream savedOut = System.out;
        PrintStream savedErr = System.err;
        String savedEsPathHome = System.getProperty("es.path.home");
        String savedCliName = System.getProperty("cli.name");
        String savedCliLibs = System.getProperty("cli.libs");
        String savedRedirect = System.getProperty("cli.redirectStdoutToStderr");
        try {
            System.setOut(new PrintStream(stdoutCapture, true, StandardCharsets.UTF_8));
            System.setErr(new PrintStream(stderrCapture, true, StandardCharsets.UTF_8));
            System.setProperty("es.path.home", esHome.toString());
            System.setProperty("cli.name", "redirect-test");
            System.setProperty("cli.libs", "");
            System.setProperty("cli.redirectStdoutToStderr", "true");
            CliToolLauncher.main(new String[0]);
        } finally {
            System.setOut(savedOut);
            System.setErr(savedErr);
            restoreOrClear("es.path.home", savedEsPathHome);
            restoreOrClear("cli.name", savedCliName);
            restoreOrClear("cli.libs", savedCliLibs);
            restoreOrClear("cli.redirectStdoutToStderr", savedRedirect);
        }
        byte[] raw = stderrCapture.toByteArray();
        byte[] errorOutputBytes = RedirectTestCommand.ERROR_OUTPUT.getBytes(StandardCharsets.UTF_8);
        int idx = indexOf(raw, errorOutputBytes);
        assertThat("error output should appear in muxed stream", idx >= 0, is(true));
        assertThat("stderr-destined output should be in STDERR_MODE", activeModeAt(raw, idx), equalTo(OutputStreamMux.STDERR_MODE));
    }

    /**
     * Returns the active mode at position {@code dataIdx} by scanning backwards for
     * the most recent mode marker. Defaults to STDOUT_MODE if none found.
     */
    private static byte activeModeAt(byte[] raw, int dataIdx) {
        for (int i = dataIdx - 1; i >= 0; i--) {
            if (raw[i] == OutputStreamMux.STDOUT_MODE || raw[i] == OutputStreamMux.STDERR_MODE) {
                return raw[i];
            }
        }
        return OutputStreamMux.STDOUT_MODE;
    }

    /**
     * Finds the first occurrence of {@code needle} in {@code haystack}, or -1 if not found.
     */
    private static int indexOf(byte[] haystack, byte[] needle) {
        outer: for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static void restoreOrClear(String key, String value) {
        if (value != null) {
            System.setProperty(key, value);
        } else {
            System.clearProperty(key);
        }
    }
}
