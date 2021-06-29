/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.java_version_checker;

import java.util.Arrays;
import java.util.Locale;

/**
 * Simple program that checks if the runtime Java version is at least 1.8.
 */
final class JavaVersionChecker {

    private JavaVersionChecker() {}

    /**
     * The main entry point. The exit code is 0 if the Java version is at least 1.8, otherwise the exit code is 1.
     *
     * @param args the args to the program which are rejected if not empty
     */
    public static void main(final String[] args) {
        // no leniency!
        if (args.length != 0) {
            throw new IllegalArgumentException("expected zero arguments but was " + Arrays.toString(args));
        }
        if (JavaVersion.compare(JavaVersion.CURRENT, JavaVersion.JAVA_11) < 0) {
            final String message = String.format(
                Locale.ROOT,
                "the minimum required Java version is 11; your Java version from [%s] does not meet this requirement",
                System.getProperty("java.home")
            );
            errPrintln(message);
            exit(1);
        }
        exit(0);
    }

    /**
     * Prints a string and terminates the line on standard error.
     *
     * @param message the message to print
     */
    @SuppressForbidden(reason = "System#err")
    static void errPrintln(final String message) {
        System.err.println(message);
    }

    /**
     * Exit the VM with the specified status.
     *
     * @param status the status
     */
    @SuppressForbidden(reason = "System#exit")
    static void exit(final int status) {
        System.exit(status);
    }

}
