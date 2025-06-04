/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class KqlParsingException extends ElasticsearchException {

    private final int line;
    private final int charPositionInLine;

    public KqlParsingException(String message, Exception cause, int line, int charPositionInLine) {
        super(message, cause);
        this.line = line;
        this.charPositionInLine = charPositionInLine;
    }

    public KqlParsingException(String message, int line, int charPositionInLine, Object... args) {
        super(message, args);
        this.line = line;
        this.charPositionInLine = charPositionInLine;
    }

    public KqlParsingException(String message, Throwable cause, int line, int charPositionInLine, Object... args) {
        super(message, cause, args);
        this.line = line;
        this.charPositionInLine = charPositionInLine;
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
    public String getMessage() {
        return format("line {}:{}: {}", getLineNumber(), getColumnNumber(), getErrorMessage());
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
