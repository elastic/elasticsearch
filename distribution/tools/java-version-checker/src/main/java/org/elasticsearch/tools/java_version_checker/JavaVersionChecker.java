/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tools.java_version_checker;

import java.util.Arrays;
import java.util.Locale;

/**
 * Java 8 compatible main to check the runtime version
 */
final class JavaVersionChecker {

    private JavaVersionChecker() {}

    public static void main(final String[] args) {
        // no leniency!
        if (args.length != 0) {
            throw new IllegalArgumentException("expected zero arguments but was " + Arrays.toString(args));
        }

        final int MIN_VERSION = 21;
        final int version;
        String versionString = System.getProperty("java.specification.version");
        if (versionString.equals("1.8")) {
            version = 8;
        } else {
            version = Integer.parseInt(versionString);
        }
        if (version >= MIN_VERSION) {
            return;
        }

        final String message = String.format(
            Locale.ROOT,
            "The minimum required Java version is %d; your Java version %d from [%s] does not meet that requirement.",
            MIN_VERSION,
            version,
            System.getProperty("java.home")
        );
        System.err.println(message);
        System.exit(1);
    }
}
