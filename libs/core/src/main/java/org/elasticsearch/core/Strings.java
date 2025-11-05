/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.util.Locale;

/**
 * Utility methods for String operations.
 *
 * <p>This class provides convenient methods for common string manipulation tasks,
 * ensuring consistent behavior across the Elasticsearch codebase.
 */
public class Strings {

    /**
     * Returns a formatted string using the specified format string and arguments.
     *
     * <p>This method calls {@link String#format(Locale, String, Object...)} with
     * {@link Locale#ROOT} to ensure consistent locale-independent formatting.
     * If the format string is incorrect, this method returns the format string
     * unchanged without populating its variable placeholders, and triggers an
     * assertion error in development environments.
     *
     * @param format the format string
     * @param args the arguments referenced by the format specifiers in the format string
     * @return the formatted string, or the original format string if formatting fails
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * String result = Strings.format("Hello %s, you have %d messages", "John", 5);
     * // Returns: "Hello John, you have 5 messages"
     * }</pre>
     */
    public static String format(String format, Object... args) {
        try {
            return String.format(Locale.ROOT, format, args);
        } catch (Exception e) {
            assert false : "Exception thrown when formatting [" + format + "]. " + e.getClass().getCanonicalName() + ". " + e.getMessage();
            return format;
        }
    }
}
