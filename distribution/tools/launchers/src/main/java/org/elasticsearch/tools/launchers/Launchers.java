/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.launchers;

import org.elasticsearch.tools.java_version_checker.SuppressForbidden;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;

/**
 * Utility methods for launchers.
 */
final class Launchers {

    /**
     * Prints a string and terminates the line on standard output.
     *
     * @param message the message to print
     */
    @SuppressForbidden(reason = "System#out")
    static void outPrintln(final String message) {
        System.out.println(message);
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

    @SuppressForbidden(reason = "Files#createTempDirectory(String, FileAttribute...)")
    static Path createTempDirectory(final String prefix, final FileAttribute<?>... attrs) throws IOException {
        return Files.createTempDirectory(prefix, attrs);
    }

}
