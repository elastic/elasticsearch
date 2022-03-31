/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.launcher;

import java.util.Locale;

public class Launcher {
    public static void main(String[] args) {
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
