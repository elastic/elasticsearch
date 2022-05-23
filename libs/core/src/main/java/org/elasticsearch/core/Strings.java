/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.util.Locale;

/**
 * Utilities related to String class
 */
public class Strings {

    /**
     * Returns a formatted string using the specified format string and
     * arguments.
     *
     * This method calls {@link String#format(Locale, String, Object...)}
     * with Locale.ROOT
     */
    public static String format(String format, Object... args) {
        return String.format(Locale.ROOT, format, args);
    }
}
