/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.sql.type.Schema;

import java.util.List;

public class ListRowSet extends AbstractRowSet implements SchemaRowSet {

    private final Schema schema;
    private final List<List<?>> list;
    private final int columnCount;
    private int pos = 0;

    ListRowSet(Schema schema, List<List<?>> list) {
        this(schema, list, schema.size());
    }

    ListRowSet(Schema schema, List<List<?>> list, int columnCount) {
        this.schema = schema;
        this.columnCount = columnCount;
        this.list = list;
    }


    @Override
    protected boolean doHasCurrent() {
        return pos < size();
    }

    @Override
    protected boolean doNext() {
        if (pos + 1 < size()) {
            pos++;
            return true;
        }
        return false;
    }

    @Override
    protected Object getColumn(int index) {
        return list.get(pos).get(index);
    }

    @Override
    protected void doReset() {
        pos = 0;
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public int columnCount() {
        return columnCount;
    }
}
