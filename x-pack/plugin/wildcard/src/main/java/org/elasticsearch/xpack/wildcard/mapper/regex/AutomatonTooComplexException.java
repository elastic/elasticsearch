/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper.regex;

/**
 * Thrown when the automaton is too complex to convert to ngrams (as measured by
 * maxExpand).
 */
public class AutomatonTooComplexException extends IllegalArgumentException {
    /**
     * Build it.
     */
    public AutomatonTooComplexException() {
        super("The supplied automaton is too complex to extract ngrams");
    }
}
