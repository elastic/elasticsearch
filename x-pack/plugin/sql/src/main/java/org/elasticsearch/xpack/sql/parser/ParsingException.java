/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.sql.ClientSqlException;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.Locale;

public class ParsingException extends ClientSqlException {
    private final int line;
    private final int charPositionInLine;

    public ParsingException(String message, Exception cause, int line, int charPositionInLine) {
        super(message, cause);
        this.line = line;
        this.charPositionInLine = charPositionInLine;
    }

    ParsingException(String message, Object... args) {
        this(Location.EMPTY, message, args);
    }

    public ParsingException(Location nodeLocation, String message, Object... args) {
        super(message, args);
        this.line = nodeLocation.getLineNumber();
        this.charPositionInLine = nodeLocation.getColumnNumber();
    }

    public ParsingException(Exception cause, Location nodeLocation, String message, Object... args) {
        super(cause, message, args);
        this.line = nodeLocation.getLineNumber();
        this.charPositionInLine = nodeLocation.getColumnNumber();
    }

    public int getLineNumber() {
        return line;
    }

    public int getColumnNumber() {
        return charPositionInLine + 1;
    }

    public String getErrorMessage() {
        return super.getMessage();
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    public String getMessage() {
        return String.format(Locale.ROOT, "line %s:%s: %s", getLineNumber(), getColumnNumber(), getErrorMessage());
    }
}
