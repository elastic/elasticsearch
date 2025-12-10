/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

/**
 * Warnings returned when loading values for ESQL. These are returned as HTTP 299 headers like so:
 * <pre>{@code
 *  < Warning: 299 Elasticsearch-${ver} "No limit defined, adding default limit of [1000]"
 *  < Warning: 299 Elasticsearch-${ver} "Line 1:27: evaluation of [a + 1] failed, treating result as null. Only first 20 failures recorded."
 *  < Warning: 299 Elasticsearch-${ver} "Line 1:27: java.lang.IllegalArgumentException: single-value function encountered multi-value"
 * }</pre>
 */
public interface Warnings {
    /**
     * Register a warning. ESQL deduplicates and limits the number of warnings returned so it should
     * be fine to blast as many warnings into this as you encounter.
     * @param exceptionClass The class of exception. Pick the same exception you'd use if you were
     *                       throwing it back over HTTP.
     * @param message The message to the called. ESQL's warnings machinery attaches the location
     *                in the original query and the text so there isn't any need to describe the
     *                function in this message.
     */
    void registerException(Class<? extends Exception> exceptionClass, String message);

    /**
     * Register the canonical warning for when a single-value only function encounters
     * a multivalued field.
     */
    static void registerSingleValueWarning(Warnings warnings) {
        warnings.registerException(IllegalArgumentException.class, "single-value function encountered multi-value");
    }
}
