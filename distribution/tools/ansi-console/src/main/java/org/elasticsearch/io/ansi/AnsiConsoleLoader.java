/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.io.ansi;

import org.elasticsearch.bootstrap.ConsoleLoader;
import org.fusesource.jansi.AnsiConsole;
import org.fusesource.jansi.AnsiPrintStream;
import org.fusesource.jansi.AnsiType;

import java.util.function.Supplier;

/**
 * Loads the {@link AnsiConsole} and checks whether it meets our requirements for a "Console".
 * @see org.elasticsearch.bootstrap.ConsoleLoader
 */
public class AnsiConsoleLoader implements Supplier<ConsoleLoader.Console> {

    public ConsoleLoader.Console get() {
        final AnsiPrintStream out = AnsiConsole.out();
        if (isValidConsole(out)) {
            // TODO use reflection to surface the Charset, in order to verify that the unicode code points can be printed
            return new ConsoleLoader.Console(out, () -> out.getTerminalWidth());
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
}
