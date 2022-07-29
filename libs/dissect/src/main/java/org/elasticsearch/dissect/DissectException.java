/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dissect;

/**
 * Parent class for all dissect related exceptions. Consumers may catch this exception or more specific child exceptions.
 */
public abstract class DissectException extends RuntimeException {
    DissectException(String message) {
        super(message);
    }

    /**
     * Error while parsing a dissect pattern
     */
    static class PatternParse extends DissectException {
        PatternParse(String pattern, String reason) {
            super("Unable to parse pattern: " + pattern + " Reason: " + reason);
        }
    }

    /**
     * Error while parsing a dissect key
     */
    static class KeyParse extends DissectException {
        KeyParse(String key, String reason) {
            super("Unable to parse key: " + key + " Reason: " + reason);
        }
    }

    /**
     * Unable to find a match between pattern and source string
     */
    static class FindMatch extends DissectException {
        FindMatch(String pattern, String source) {
            super("Unable to find match for dissect pattern: " + pattern + " against source: " + source);

        }
    }
}
