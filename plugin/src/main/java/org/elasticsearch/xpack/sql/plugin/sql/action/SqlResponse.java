/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.session.RowSetCursor;

public class SqlResponse extends ActionResponse {

    private String sessionId;
    private RowSetCursor rowCursor;

    public SqlResponse() {}

    public SqlResponse(String sessionId, RowSetCursor rowCursor) {
        this.sessionId = sessionId;
        this.rowCursor = rowCursor;
    }

    public RowSetCursor rowSetCursor() {
        return rowCursor;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("only local transport");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("only local transport");
    }
}
