/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tools.java_version_checker;

import java.util.Arrays;
import java.util.Locale;

/**
 * Simple program that checks if the runtime Java version is at least 1.8.
 */
final class JavaVersionChecker {

    private JavaVersionChecker() {
    }

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
                    System.getProperty("java.home"));
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
