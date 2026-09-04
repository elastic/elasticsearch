/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli.terminal;

import java.io.Console;

final class ConsoleUtil {

    private ConsoleUtil() {}

    /**
     * JDK >= 22 returns a console even if the terminal is redirected unless using -Djdk.console=java.base
     * https://bugs.openjdk.org/browse/JDK-8308591
     */
    static Console detectTerminal() {
        Console console = System.console();
        return console != null && console.isTerminal() ? console : null;
    }
}
