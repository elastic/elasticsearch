/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.cli.UserException;

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
     *
     * @throws UserException if there is a problem reading the password,
     *      for instance, the user {@code ctrl-c}s while we're waiting
     *      or they send an EOF
     * @return the password the user typed, never null
     */
    String readPassword(String prompt) throws UserException;

    /**
     * Reads the line from the terminal.
     *
     * @return {@code null} if the user closes the terminal while we're
     * waiting for the line, {@code ""} if the use {@code ctrl-c}s while
     * we're waiting, the line they typed otherwise
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
