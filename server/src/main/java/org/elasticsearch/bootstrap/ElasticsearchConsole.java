/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Locale;

/**
 * A basic "console" class for writing to the user's console without writing to the logfile.
 * This is to be used in very exceptional circumstances (e.g. printing out sensitive values that ought not to be logged).
 * It is not to be confused with the java {@link java.io.Console} (see {@link System#console()}) which will generally be {@code null}
 * when running Elasticsearch from the command line
 */
public class ElasticsearchConsole {

    private static ElasticsearchConsole instance = null;

    private final PrintStream stdout;

    /**
     * Returns the current console. Will be {@code null} if there is no console.
     * It is very common for there not to be a console (for example, because Elasticsearch is running as a service) - callers should be
     * prepared to handle a null console instance.
     */
    @Nullable
    public static ElasticsearchConsole getInstance() {
        return instance;
    }

    ElasticsearchConsole(PrintStream stdout) {
        this.stdout = stdout;
    }

    public void println(String line) throws IOException {
        stdout.println(line);
    }

    public void printf(String format, Object... args) throws IOException {
        stdout.printf(Locale.ROOT, format, args);
    }

    @SuppressForbidden(reason = "Initialize using System.out")
    static void init() {
        instance = new ElasticsearchConsole(System.out);
    }
}
