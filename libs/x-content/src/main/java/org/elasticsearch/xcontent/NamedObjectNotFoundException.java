/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

/**
 * Thrown when {@link NamedXContentRegistry} cannot locate a named object to
 * parse for a particular name
 */
public class NamedObjectNotFoundException extends XContentParseException {
    private final Iterable<String> candidates;

    public NamedObjectNotFoundException(XContentLocation location, String message, Iterable<String> candidates) {
        super(location, message);
        this.candidates = candidates;
    }

    /**
     * The possible matches.
     */
    public Iterable<String> getCandidates() {
        return candidates;
    }
}
