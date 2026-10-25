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
 * Utilities related to String class
 */
public class Strings {

    /** Maximum number of input chars kept in an error message. */
    public static final int MAX_INPUT_LENGTH_IN_ERROR = 1024;

    /**
     * Cuts user input echoed in an error message to {@link #MAX_INPUT_LENGTH_IN_ERROR} chars, adding "..." when it is cut. Does not split
     * a surrogate pair.
     */
    public static String truncateForError(String input) {
        if (input == null || input.length() <= MAX_INPUT_LENGTH_IN_ERROR) {
            return input;
        }
        int end = MAX_INPUT_LENGTH_IN_ERROR;
        if (Character.isHighSurrogate(input.charAt(end - 1))) {
            end--;
        }
        return input.substring(0, end) + "...";
    }

    /**
     * Returns a formatted string using the specified format string and
     * arguments.
     * <p>
     * This method calls {@link String#format(Locale, String, Object...)}
     * with Locale.ROOT
     * If format is incorrect the function will return format without populating
     * its variable placeholders.
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
