/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.sql.ClientSqlException;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.Node;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class AnalysisException extends ClientSqlException {

    private final int line;
    private final int column;

    public AnalysisException(Node<?> source, String message, Object... args) {
        super(message, args);

        Location loc = Location.EMPTY;
        if (source != null && source.source() != null) {
            loc = source.source().source();
        }
        this.line = loc.getLineNumber();
        this.column = loc.getColumnNumber();
    }

    public AnalysisException(Node<?> source, String message, Throwable cause) {
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
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    public String getMessage() {
        return format("line {}:{}: {}", getLineNumber(), getColumnNumber(), super.getMessage());
    }
}
