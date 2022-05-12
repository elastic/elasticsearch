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
 * Java 7 compatible main which exits with an error.
 */
final class JavaVersionChecker {

    private JavaVersionChecker() {}

    public static void main(final String[] args) {
        // no leniency!
        if (args.length != 0) {
            throw new IllegalArgumentException("expected zero arguments but was " + Arrays.toString(args));
        }
        final String message = String.format(
            Locale.ROOT,
            "The minimum required Java version is 17; your Java version %s from [%s] does not meet that requirement.",
            System.getProperty("java.specification.version"),
            System.getProperty("java.home")
        );
        System.err.println(message);
        System.exit(1);
    }
}
