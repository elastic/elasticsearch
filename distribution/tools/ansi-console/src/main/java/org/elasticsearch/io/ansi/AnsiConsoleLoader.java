/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.io.ansi;

import org.fusesource.jansi.AnsiConsole;
import org.fusesource.jansi.AnsiPrintStream;
import org.fusesource.jansi.AnsiType;

import java.io.PrintStream;
import java.util.function.Supplier;

/**
 * Loads the({@link PrintStream} print stream) from {@link AnsiConsole} and checks whether it meets our requirements for a "Console".
 * @see org.elasticsearch.bootstrap.ConsoleLoader
 */
public class AnsiConsoleLoader implements Supplier<PrintStream> {

    public PrintStream get() {
        final AnsiPrintStream out = AnsiConsole.out();
        if (isValidConsole(out)) {
            return out;
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
