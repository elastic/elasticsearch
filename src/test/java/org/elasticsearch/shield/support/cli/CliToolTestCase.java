/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support.cli;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.PrintWriter;

/**
 *
 */
public class CliToolTestCase extends ElasticsearchTestCase {

    protected static String[] args(String command) {
        if (!Strings.hasLength(command)) {
            return Strings.EMPTY_ARRAY;
        }
        return command.split("\\s+");
    }

    public static class TerminalMock extends Terminal {

        @Override
        public void println() {
        }

        @Override
        public void println(String msg, Object... args) {
        }

        @Override
        public String readText(String text, Object... args) {
            return null;
        }

        @Override
        public char[] readSecret(String text, Object... args) {
            return new char[0];
        }

        @Override
        public void print(String msg, Object... args) {

        }

        @Override
        public PrintWriter writer() {
            return null;
        }
    }

}
