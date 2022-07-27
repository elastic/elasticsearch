/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestTerminal implements CliTerminal {

    private StringBuilder stringBuilder = new StringBuilder();
    private boolean closed = false;
    private Iterator<String> inputLines;

    public TestTerminal(String... inputLines) {
        this.inputLines = Arrays.asList(inputLines).iterator();
    }

    @Override
    public LineBuilder line() {
        return new LineBuilder() {

            @Override
            public LineBuilder text(String text) {
                stringBuilder.append(text);
                return this;
            }

            @Override
            public LineBuilder em(String text) {
                stringBuilder.append("<em>").append(text).append("</em>");
                return this;
            }

            @Override
            public LineBuilder error(String text) {
                stringBuilder.append("<b>").append(text).append("</b>");
                return this;
            }

            @Override
            public LineBuilder param(String text) {
                stringBuilder.append("<i>").append(text).append("</i>");
                return this;
            }

            @Override
            public void ln() {
                stringBuilder.append("\n");
            }

            @Override
            public void end() {
                stringBuilder.append("<flush/>");
            }
        };
    }

    @Override
    public void print(String text) {
        stringBuilder.append(text);
    }

    @Override
    public void println(String text) {
        stringBuilder.append(text);
        stringBuilder.append("\n");
    }

    @Override
    public void error(String type, String message) {
        stringBuilder.append("<b>").append(type).append(" [</b>");
        stringBuilder.append("<i>").append(message).append("</i>");
        stringBuilder.append("<b>]</b>\n");
    }

    @Override
    public void println() {
        stringBuilder.append("\n");
    }

    @Override
    public void clear() {
        stringBuilder = new StringBuilder();
    }

    @Override
    public void flush() {
        stringBuilder.append("<flush/>");
    }

    @Override
    public void printStackTrace(Exception ex) {
        stringBuilder.append("<stack/>");
    }

    @Override
    public String readPassword(String prompt) {
        return "password";
    }

    @Override
    public String readLine(String prompt) {
        assertTrue(inputLines.hasNext());
        return inputLines.next();
    }

    @Override
    public void close() throws IOException {
        assertFalse(closed);
        closed = true;
    }

    @Override
    public String toString() {
        return stringBuilder.toString();
    }
}
