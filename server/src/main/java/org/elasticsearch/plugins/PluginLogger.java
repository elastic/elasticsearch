/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.io.Console;

/**
 * This interface abstracts the logging destination for a plugin action e.g. installing and removing.
 * From the command line, this would be backed by a {@link org.elasticsearch.cli.Terminal} instance,
 * but in the Elasticsearch server it could be backed by a log4j logger.
 */
public interface PluginLogger {

    /**
     * Log a message with low priority to the standard output.
     * @param message the message to log
     */
    void debug(String message);

    /**
     * Log a message with normal priority to the standard output.
     * @param message the message to log
     */
    void info(String message);

    /**
     * Log a message with normal priority to the error output.
     * @param message the message to log
     */
    void warn(String message);

    /**
     * Log a message with high priority to the error output.
     * @param message the message to log
     */
    void error(String message);

    /**
     * Displays a prompt and reads a line of input from the terminal. Not guaranteed to be implemented.
     * See {@link Console#readLine()}.
     * @param prompt the prompt text to display.
     * @return the line read from the terminal.
     * @throws UnsupportedOperationException if the logger doesn't support this method.
     */
    default String readText(String prompt) {
        throw new UnsupportedOperationException();
    }
}
