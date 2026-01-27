/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Iterator;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class ParsingException extends EsqlClientException {
    private final int line;
    private final int charPositionInLine;

    public ParsingException(String message, Exception cause, int line, int charPositionInLine) {
        super(message, cause);
        this.line = line;
        this.charPositionInLine = charPositionInLine + 1;
    }

    /**
     * To be used only if the exception cannot be associated with a specific position in the query.
     * Error message will start with {@code line -1:-1:} instead of using specific location.
     */
    public ParsingException(String message, Object... args) {
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

    private ParsingException(int line, int charPositionInLine, String message, Object... args) {
        super(message, args);
        this.line = line;
        this.charPositionInLine = charPositionInLine;
    }

    /**
     * Combine multiple {@code ParsingException} into one, this is used by {@code LogicalPlanBuilder} to
     * consolidate multiple named parameters related {@code ParsingException}.
     */
    public static ParsingException combineParsingExceptions(Iterator<ParsingException> parsingExceptions) {
        StringBuilder message = new StringBuilder();
        int i = 0;
        int line = -1;
        int charPositionInLine = -1;

        while (parsingExceptions.hasNext()) {
            ParsingException e = parsingExceptions.next();
            if (i > 0) {
                message.append("; ");
                message.append(e.getMessage());
            } else {
                // line and column numbers are the associated with the first error
                line = e.getLineNumber();
                charPositionInLine = e.getColumnNumber();
                message.append(e.getErrorMessage());
            }
            i++;
        }
        return new ParsingException(line, charPositionInLine, message.toString());
    }

    public int getLineNumber() {
        return line;
    }

    public int getColumnNumber() {
        return charPositionInLine;
    }

    public String getErrorMessage() {
        return super.getMessage();
    }

    @Override
    public String getMessage() {
        return format("line {}:{}: {}", getLineNumber(), getColumnNumber(), getErrorMessage());
    }
}
