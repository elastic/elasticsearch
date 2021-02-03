/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import java.util.ServiceLoader;

/**
 * Extension point to customize the error message for unknown fields. We expect
 * Elasticsearch to plug a fancy implementation that uses Lucene's spelling
 * correction infrastructure to suggest corrections.
 */
public interface ErrorOnUnknown {
    /**
     * The implementation of this interface that was loaded from SPI.
     */
    ErrorOnUnknown IMPLEMENTATION = findImplementation();

    /**
     * Build the error message to use when {@link ObjectParser} encounters an unknown field.
     * @param parserName the name of the thing we're parsing
     * @param unknownField the field that we couldn't recognize
     * @param candidates the possible fields
     */
    String errorMessage(String parserName, String unknownField, Iterable<String> candidates);

    /**
     * Priority that this error message handler should be used.
     */
    int priority();

    private static ErrorOnUnknown findImplementation() {
        ErrorOnUnknown best = new ErrorOnUnknown() {
            @Override
            public String errorMessage(String parserName, String unknownField, Iterable<String> candidates) {
                return "[" + parserName + "] unknown field [" + unknownField + "]";
            }

            @Override
            public int priority() {
                return Integer.MIN_VALUE;
            }
        };
        for (ErrorOnUnknown c : ServiceLoader.load(ErrorOnUnknown.class)) {
            if (best.priority() < c.priority()) {
                best = c;
            }
        }
        return best;
    }
}
