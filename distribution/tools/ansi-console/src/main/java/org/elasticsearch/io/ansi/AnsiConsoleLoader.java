/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.io.ansi;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.bootstrap.ConsoleLoader;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;
import org.fusesource.jansi.AnsiPrintStream;
import org.fusesource.jansi.AnsiType;
import org.fusesource.jansi.io.AnsiOutputStream;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.function.Supplier;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Loads the {@link AnsiConsole} and checks whether it meets our requirements for a "Console".
 * @see org.elasticsearch.bootstrap.ConsoleLoader
 */
public class AnsiConsoleLoader implements Supplier<ConsoleLoader.Console> {

    private static final Logger logger = getLogger(AnsiConsoleLoader.class);

    public ConsoleLoader.Console get() {
        final AnsiPrintStream out = AnsiConsole.out();
        return newConsole(out);
    }

    // package-private for tests
    static @Nullable ConsoleLoader.Console newConsole(AnsiPrintStream out) {
        if (isValidConsole(out)) {
            // virtual terminal does support ANSI escape sequences, but the JVM must toggle a mode
            // option on the console using the Kernel32 API, which JANSI knows to do, but ES currently lacks
            // the testing infra to assert the behavior
            boolean ansiEnabled = Ansi.isEnabled() && out.getType() != AnsiType.VirtualTerminal;
            return new ConsoleLoader.Console(out, () -> out.getTerminalWidth(), ansiEnabled, tryExtractPrintCharset(out));
        } else {
            return null;
        }
    }

    private static boolean isValidConsole(AnsiPrintStream out) {
        return out != null // cannot load stdout
            && out.getType() != AnsiType.Redirected // output is a pipe (etc)
            && out.getType() != AnsiType.Unsupported // could not determine terminal type
            && out.getTerminalWidth() > 0 // docker, non-terminal logs
        ;
    }

    /**
     * Uses reflection on the JANSI lib in order to expose the {@code Charset} used to encode the console's print stream.
     * The {@code Charset} is not otherwise exposed by the library, and this avoids replicating the charset selection logic in out code.
     */
    @SuppressForbidden(reason = "Best effort exposing print stream's charset with reflection")
    @Nullable
    private static Charset tryExtractPrintCharset(AnsiPrintStream ansiPrintStream) {
        try {
            Method getOutMethod = ansiPrintStream.getClass().getDeclaredMethod("getOut");
            getOutMethod.setAccessible(true);
            AnsiOutputStream ansiOutputStream = (AnsiOutputStream) getOutMethod.invoke(ansiPrintStream);
            Field charsetField = ansiOutputStream.getClass().getDeclaredField("cs");
            charsetField.setAccessible(true);
            return (Charset) charsetField.get(ansiOutputStream);
        } catch (Throwable t) {
            // has the library been upgraded and it now doesn't expose the same fields with the same names?
            // is the Security Manager installed preventing the access
            logger.info("Failed to detect JANSI's print stream encoding", t);
            return null;
        }
    }
}
