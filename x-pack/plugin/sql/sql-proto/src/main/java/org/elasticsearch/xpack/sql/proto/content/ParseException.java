/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.content;

public class ParseException extends IllegalArgumentException {

    private final ContentLocation location;

    public ParseException(String message) {
        this(null, message);
    }

    public ParseException(ContentLocation location, String message) {
        super(message);
        this.location = location != null ? location : ContentLocation.UNKNOWN;
    }

    public ParseException(ContentLocation location, String message, Exception cause) {
        super(message, cause);
        this.location = location != null ? location : ContentLocation.UNKNOWN;
    }

    public ContentLocation location() {
        return location;
    }

    @Override
    public String getMessage() {
        return location + super.getMessage();
    }
}
