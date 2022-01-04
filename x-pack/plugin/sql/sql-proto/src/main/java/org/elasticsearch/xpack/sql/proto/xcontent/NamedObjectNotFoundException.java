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
