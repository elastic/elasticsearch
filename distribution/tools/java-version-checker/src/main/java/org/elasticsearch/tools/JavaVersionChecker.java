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

package org.elasticsearch.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Simple program that checks if the runtime Java version is at least 1.8.
 */
final class JavaVersionChecker {

    private JavaVersionChecker() {
    }

    private static final List<Integer> JAVA_8 = Arrays.asList(1, 8);

    /**
     * The main entry point. The exit code is 0 if the Java version is at least 1.8, otherwise the exit code is 1.
     *
     * @param args the args to the program which are rejected if not empty
     */
    public static void main(final String[] args) {
        // no leniency!
        if (args.length != 0) {
            throw new IllegalArgumentException("expected zero arguments but was: " + Arrays.toString(args));
        }
        final String javaSpecificationVersion = System.getProperty("java.specification.version");
        final List<Integer> current = parse(javaSpecificationVersion);
        if (compare(current, JAVA_8) < 0) {
            final String message = String.format(
                    Locale.ROOT,
                    "the minimum required Java version is 8; your Java version from [%s] does not meet this requirement",
                    System.getProperty("java.home"));
            println(message);
            exit(1);
        }
        exit(0);
    }

    private static List<Integer> parse(final String value) {
        if (!value.matches("^0*[0-9]+(\\.[0-9]+)*$")) {
            throw new IllegalArgumentException(value);
        }

        final List<Integer> version = new ArrayList<Integer>();
        final String[] components = value.split("\\.");
        for (final String component : components) {
            version.add(Integer.valueOf(component));
        }
        return version;
    }

    private static int compare(final List<Integer> left, final List<Integer> right) {
        // lexicographically compare two lists, treating missing entries as zeros
        final int len = Math.max(left.size(), right.size());
        for (int i = 0; i < len; i++) {
            final int l = (i < left.size()) ? left.get(i) : 0;
            final int r = (i < right.size()) ? right.get(i) : 0;
            if (l < r) {
                return -1;
            }
            if (r < l) {
                return 1;
            }
        }
        return 0;
    }

    @SuppressForbidden(reason = "System#err")
    private static void println(String message) {
        System.err.println(message);
    }

    @SuppressForbidden(reason = "System#exit")
    private static void exit(final int status) {
        System.exit(status);
    }

}
