/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.logging.Logger;

import static org.hamcrest.Matchers.equalTo;

public class JavaUtilLoggingFormatTests extends ESTestCase {
    @BeforeClass
    public static void init() {
        assert "false".equals(System.getProperty("tests.security.manager")) : "-Dtests.security.manager=false has to be set";
    }

    public void testJavaUtilLoggingFormat() {
        String key = key(SystemJvmOptions.javaUtilLoggingDefaultFormat());
        String value = value(SystemJvmOptions.javaUtilLoggingDefaultFormat());
        System.setProperty(key, value);

        final ByteArrayOutputStream myOut = new ByteArrayOutputStream();
        PrintStream err = System.err;
        PrintStream out = System.out;
        try {
            System.setErr(new PrintStream(myOut));
            System.setOut(new PrintStream(myOut));


            Logger.getLogger("any").info("message");

            assertThat(myOut.toString(), equalTo("[INFO][any] message\n"));
        } finally {
            System.setOut(out);
            System.setErr(err);
        }
    }

    public String key(String s) {
        return s.split("=")[0].substring(2);
    }

    public String value(String s) {
        return s.split("=")[1];
    }
}
