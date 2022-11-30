/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.java_version_checker;

import java.util.Arrays;

/**
 * Java 17 compatible main which just exits without error.
 */
final class JavaVersionChecker {

    private JavaVersionChecker() {}

    public static void main(final String[] args) {
        // no leniency!
        if (args.length != 0) {
            throw new IllegalArgumentException("expected zero arguments but was " + Arrays.toString(args));
        }
    }
}
