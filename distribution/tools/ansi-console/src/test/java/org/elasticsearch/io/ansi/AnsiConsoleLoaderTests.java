/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.io.ansi;

import org.elasticsearch.test.ESTestCase;
import org.fusesource.jansi.AnsiColors;
import org.fusesource.jansi.AnsiMode;
import org.fusesource.jansi.AnsiPrintStream;
import org.fusesource.jansi.AnsiType;
import org.fusesource.jansi.io.AnsiOutputStream;
import org.fusesource.jansi.io.AnsiProcessor;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;

public class AnsiConsoleLoaderTests extends ESTestCase {

    private static final AnsiType[] SUPPORTED_TERMINAL_TYPES = new AnsiType[] {
        AnsiType.VirtualTerminal,
        AnsiType.Native,
        AnsiType.Emulation };

    private static final AnsiOutputStream.IoRunnable NO_OP_RUNNABLE = () -> {};

    public void testNullOutputIsNotConsole() {
        assertThat(AnsiConsoleLoader.isValidConsole(null), is(false));
    }

    public void testRedirectedOutputIsNotConsole() {
        try (AnsiPrintStream ansiPrintStream = buildStream(AnsiType.Redirected, randomIntBetween(80, 120))) {
            assertThat(AnsiConsoleLoader.isValidConsole(ansiPrintStream), is(false));
        }
    }

    public void testUnsupportedTerminalIsNotConsole() {
        try (AnsiPrintStream ansiPrintStream = buildStream(AnsiType.Unsupported, randomIntBetween(80, 120))) {
            assertThat(AnsiConsoleLoader.isValidConsole(ansiPrintStream), is(false));
        }
    }

    public void testZeroWidthTerminalIsNotConsole() {
        try (AnsiPrintStream ansiPrintStream = buildStream(randomFrom(SUPPORTED_TERMINAL_TYPES), 0)) {
            assertThat(AnsiConsoleLoader.isValidConsole(ansiPrintStream), is(false));
        }
    }

    public void testStandardTerminalIsConsole() {
        try (AnsiPrintStream ansiPrintStream = buildStream(randomFrom(SUPPORTED_TERMINAL_TYPES), randomIntBetween(40, 260))) {
            assertThat(AnsiConsoleLoader.isValidConsole(ansiPrintStream), is(true));
        }
    }

    private AnsiPrintStream buildStream(AnsiType type, int width) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final AnsiOutputStream ansiOutputStream = new AnsiOutputStream(
            baos,
            () -> width,
            randomFrom(AnsiMode.values()),
            new AnsiProcessor(baos),
            type,
            randomFrom(AnsiColors.values()),
            randomFrom(StandardCharsets.UTF_8, StandardCharsets.US_ASCII, StandardCharsets.UTF_16, StandardCharsets.ISO_8859_1),
            NO_OP_RUNNABLE,
            NO_OP_RUNNABLE,
            randomBoolean()
        );
        return new AnsiPrintStream(ansiOutputStream, randomBoolean());
    }

}
