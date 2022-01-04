/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.xcontent;

/**
 * NB: Light-clone from XContent library to keep JDBC driver independent.
 *
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
        return best;
    }
}
