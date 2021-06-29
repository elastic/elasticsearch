/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.eql.EqlClientException;
import org.elasticsearch.xpack.ql.tree.Source;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class ParsingException extends EqlClientException {
    private final int line;
    private final int charPositionInLine;

    public ParsingException(String message, Exception cause, int line, int charPositionInLine) {
        super(message, cause);
        this.line = line;
        this.charPositionInLine = charPositionInLine;
    }

    ParsingException(String message, Object... args) {
        this(Source.EMPTY, message, args);
    }

    public ParsingException(Source source, String message, Object... args) {
        super(message, args);
        this.line = source.source().getLineNumber();
        this.charPositionInLine = source.source().getColumnNumber();
    }

    public ParsingException(Exception cause, Source source, String message, Object... args) {
        super(cause, message, args);
        this.line = source.source().getLineNumber();
        this.charPositionInLine = source.source().getColumnNumber();
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
        return format("line {}:{}: {}", getLineNumber(), getColumnNumber(), getErrorMessage());
    }
}
