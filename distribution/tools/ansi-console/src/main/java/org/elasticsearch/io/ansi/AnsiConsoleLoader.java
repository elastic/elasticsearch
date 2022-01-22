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
        if (isValidConsole(out)) {
            return new ConsoleLoader.Console(out, () -> out.getTerminalWidth(), tryExtractPrintCharset(out));
        } else {
            return null;
        }
    }

    static boolean isValidConsole(AnsiPrintStream out) {
        return out != null // cannot load stdout
            && out.getType() != AnsiType.Redirected // output is a pipe (etc)
            && out.getType() != AnsiType.Unsupported // could not determine terminal type
            && out.getTerminalWidth() > 0 // docker, non-terminal logs
        ;
    }

    /**
     * Uses reflection on the jansi lib in order to expose the {@code Charset} used to encode the console's print stream.
     * The {@code Charset} is not otherwise exposed, and this avoids replicating the charset selection logic.
     */
    @Nullable
    static Charset tryExtractPrintCharset(AnsiPrintStream ansiPrintStream) {
        try {
            Method getOutMethod = ansiPrintStream.getClass().getDeclaredMethod("getOut");
            getOutMethod.setAccessible(true);
            AnsiOutputStream ansiOutputStream = (AnsiOutputStream) getOutMethod.invoke(ansiPrintStream);
            Field charsetField = ansiOutputStream.getClass().getDeclaredField("cs");
            charsetField.setAccessible(true);
            return (Charset) charsetField.get(ansiOutputStream);
        } catch (Exception e) {
            // has the library been upgraded and it now doesn't expose the same fields with the same names?
            logger.info("Failed to detect JANSI's print stream encoding", e);
            return null;
        }
    }
}
