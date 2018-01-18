/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import java.io.IOException;

/**
 * Represents a terminal endpoint
 */
public interface CliTerminal extends AutoCloseable {

    /**
     * Prints line with plain text
     */
    void print(String text);

    /**
     * Prints line with plain text followed by a new line
     */
    void println(String text);

    /**
     * Prints a formatted error message
     */
    void error(String type, String message);

    /**
     * Prints a new line
     */
    void println();

    /**
     * Clears the terminal
     */
    void clear();

    /**
     * Flushes the terminal
     */
    void flush();

    /**
     * Prints the stacktrace of the exception
     */
    void printStackTrace(Exception ex);

    /**
     * Prompts the user to enter the password and returns it.
     */
    String readPassword(String prompt);

    /**
     * Reads the line from the terminal.
     */
    String readLine(String prompt);

    /**
     * Creates a new line builder, which allows building a formatted lines.
     *
     * The line is not displayed until it is closed with ln() or end().
     */
    LineBuilder line();

    interface LineBuilder {
        /**
         * Adds a plain text to the line
         */
        LineBuilder text(String text);

        /**
         * Adds a text with emphasis to the line
         */
        LineBuilder em(String text);

        /**
         * Adds a text representing the error message
         */
        LineBuilder error(String text);

        /**
         * Adds a text representing a parameter of the error message
         */
        LineBuilder param(String text);

        /**
         * Adds '\n' to the line and send it to the screen.
         */
        void ln();

        /**
         * Sends the line to the screen.
         */
        void end();
    }

    @Override
    void close() throws IOException;
}
