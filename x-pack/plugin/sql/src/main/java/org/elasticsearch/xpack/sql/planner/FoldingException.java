/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.sql.SqlClientException;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class FoldingException extends SqlClientException {

    private final int line;
    private final int column;

    public FoldingException(Node<?> source, String message, Object... args) {
        super(message, args);

        Location loc = Location.EMPTY;
        if (source != null && source.source() != null) {
            loc = source.source().source();
        }
        this.line = loc.getLineNumber();
        this.column = loc.getColumnNumber();
    }

    public FoldingException(Node<?> source, String message, Throwable cause) {
        super(message, cause);

        Location loc = Location.EMPTY;
        if (source != null && source.source() != null) {
            loc = source.source().source();
        }
        this.line = loc.getLineNumber();
        this.column = loc.getColumnNumber();
    }

    public int getLineNumber() {
        return line;
    }

    public int getColumnNumber() {
        return column;
    }

    @Override
    public String getMessage() {
        return format("line {}:{}: {}", getLineNumber(), getColumnNumber(), super.getMessage());
    }
}
