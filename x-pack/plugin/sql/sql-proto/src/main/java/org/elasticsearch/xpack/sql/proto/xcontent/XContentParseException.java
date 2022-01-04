/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.xcontent;

import org.elasticsearch.xpack.sql.proto.core.Nullable;

import java.util.Optional;

/**
 * NB: Light-clone from XContent library to keep JDBC driver independent.
 *
 * Thrown when one of the XContent parsers cannot parse something.
 */
public class XContentParseException extends IllegalArgumentException {

    private final Optional<XContentLocation> location;

    public XContentParseException(String message) {
        this(null, message);
    }

    public XContentParseException(XContentLocation location, String message) {
        super(message);
        this.location = Optional.ofNullable(location);
    }

    public XContentParseException(XContentLocation location, String message, Exception cause) {
        super(message, cause);
        this.location = Optional.ofNullable(location);
    }

    public int getLineNumber() {
        return location.map(l -> l.lineNumber).orElse(-1);
    }

    public int getColumnNumber() {
        return location.map(l -> l.columnNumber).orElse(-1);
    }

    @Nullable
    public XContentLocation getLocation() {
        return location.orElse(null);
    }

    @Override
    public String getMessage() {
        return location.map(l -> "[" + l.toString() + "] ").orElse("") + super.getMessage();
    }
}
