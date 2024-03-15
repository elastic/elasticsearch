/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;

import java.util.List;

/** View over the response, that supports the xpack core transport API. */
public class EsqlResponseImpl implements EsqlResponse {

    private final EsqlQueryResponse queryResponse;
    private boolean closed;

    EsqlResponseImpl(EsqlQueryResponse queryResponse) {
        this.queryResponse = queryResponse;
    }

    @Override
    public List<? extends ColumnInfo> columns() {
        return queryResponse.columns();
    }

    @Override
    public Iterable<Iterable<Object>> rows() {
        ensureOpen();
        return () -> {
            ensureOpen();
            return queryResponse.rows().iterator();
        };
    }

    @Override
    public Iterable<Object> column(int columnIndex) {
        ensureOpen();
        return () -> {
            ensureOpen();
            return queryResponse.column(columnIndex);
        };
    }

    @Override
    public void close() {
        setClosedState();
    }

    public void setClosedState() {
        closed = true;
    }

    private void ensureOpen() {
        if (closed || queryResponse.hasReferences() == false) {
            throw new IllegalStateException("closed");
        }
    }

    @Override
    public String toString() {
        return "EsqlResponse[response=" + queryResponse + "]";
    }
}
