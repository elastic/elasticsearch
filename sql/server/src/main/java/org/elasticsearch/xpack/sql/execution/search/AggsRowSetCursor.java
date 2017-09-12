/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.xpack.sql.session.AbstractRowSetCursor;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.type.Schema;

import java.util.List;
import java.util.function.Supplier;

class AggsRowSetCursor extends AbstractRowSetCursor {

    private int row = 0;
    private final AggValues agg;
    private final List<Supplier<Object>> columns;

    AggsRowSetCursor(Schema schema, AggValues agg, List<Supplier<Object>> columns) {
        super(schema, null);
        this.agg = agg;
        this.columns = columns;
    }

    @Override
    protected Object getColumn(int column) {
        return columns.get(column).get();
    }

    @Override
    protected boolean doHasCurrent() {
        return row < size();
    }

    @Override
    protected boolean doNext() {
        return agg.nextRow();
    }

    @Override
    protected void doReset() {
        agg.reset();
    }

    @Override
    public int size() {
        return agg.size();
    }

    @Override
    public Cursor nextPageCursor() {
        return Cursor.EMPTY;
    }
}