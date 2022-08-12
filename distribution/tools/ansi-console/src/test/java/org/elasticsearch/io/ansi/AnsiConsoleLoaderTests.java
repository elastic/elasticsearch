/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.io.ansi;

import org.elasticsearch.bootstrap.ConsoleLoader;
import org.elasticsearch.test.ESTestCase;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiColors;
import org.fusesource.jansi.AnsiMode;
import org.fusesource.jansi.AnsiPrintStream;
import org.fusesource.jansi.AnsiType;
import org.fusesource.jansi.io.AnsiOutputStream;
import org.fusesource.jansi.io.AnsiProcessor;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AnsiConsoleLoaderTests extends ESTestCase {

    private static final AnsiType[] SUPPORTED_TERMINAL_TYPES = new AnsiType[] {
        AnsiType.VirtualTerminal,
        AnsiType.Native,
        AnsiType.Emulation };

    private static final AnsiOutputStream.IoRunnable NO_OP_RUNNABLE = () -> {};

    private static final Charset[] charsets = new Charset[] {
        StandardCharsets.US_ASCII,
        StandardCharsets.ISO_8859_1,
        StandardCharsets.UTF_8,
        StandardCharsets.UTF_16,
        StandardCharsets.UTF_16LE,
        StandardCharsets.UTF_16BE };

    public void testNullOutputIsNotConsole() {
        assertThat(AnsiConsoleLoader.newConsole(null), nullValue());
    }

    public void testRedirectedOutputIsNotConsole() {
        try (AnsiPrintStream ansiPrintStream = buildStream(AnsiType.Redirected, randomIntBetween(80, 120))) {
            assertThat(AnsiConsoleLoader.newConsole(ansiPrintStream), nullValue());
        }
    }

    public void testUnsupportedTerminalIsNotConsole() {
        try (AnsiPrintStream ansiPrintStream = buildStream(AnsiType.Unsupported, randomIntBetween(80, 120))) {
            assertThat(AnsiConsoleLoader.newConsole(ansiPrintStream), nullValue());
        }
    }

    public void testZeroWidthTerminalIsNotConsole() {
        try (AnsiPrintStream ansiPrintStream = buildStream(randomFrom(SUPPORTED_TERMINAL_TYPES), 0)) {
            assertThat(AnsiConsoleLoader.newConsole(ansiPrintStream), nullValue());
        }
    }

    public void testStandardTerminalIsConsole() {
        int width = randomIntBetween(40, 260);
        try (AnsiPrintStream ansiPrintStream = buildStream(randomFrom(SUPPORTED_TERMINAL_TYPES), width)) {
            ConsoleLoader.Console console = AnsiConsoleLoader.newConsole(ansiPrintStream);
            assertThat(console, notNullValue());
            assertThat(console.width().get(), is(width));
        }
    }

    public void testConsoleCharset() {
        Charset charset = randomFrom(charsets);
        try (AnsiPrintStream ansiPrintStream = buildStream(randomFrom(SUPPORTED_TERMINAL_TYPES), randomIntBetween(40, 260), charset)) {
            ConsoleLoader.Console console = AnsiConsoleLoader.newConsole(ansiPrintStream);
            assertThat(console, notNullValue());
            assertThat(console.charset(), is(charset));
        }
    }

    public void testDisableANSI() {
        Ansi.setEnabled(false);
        try (AnsiPrintStream ansiPrintStream = buildStream(randomFrom(SUPPORTED_TERMINAL_TYPES), randomIntBetween(40, 260))) {
            ConsoleLoader.Console console = AnsiConsoleLoader.newConsole(ansiPrintStream);
            assertThat(console, notNullValue());
            assertThat(console.ansiEnabled(), is(false));
        }
    }

    private AnsiPrintStream buildStream(AnsiType type, int width) {
        return buildStream(type, width, randomFrom(charsets));
    }

    private AnsiPrintStream buildStream(AnsiType type, int width, Charset cs) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final AnsiOutputStream ansiOutputStream = new AnsiOutputStream(
            baos,
            () -> width,
            randomFrom(AnsiMode.values()),
            new AnsiProcessor(baos),
            type,
            randomFrom(AnsiColors.values()),
            cs,
            NO_OP_RUNNABLE,
            NO_OP_RUNNABLE,
            randomBoolean()
        );
        return new AnsiPrintStream(ansiOutputStream, randomBoolean());
    }
}
