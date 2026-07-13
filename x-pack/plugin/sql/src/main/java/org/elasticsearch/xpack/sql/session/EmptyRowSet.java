/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.ql.type.Schema;

class EmptyRowSet extends AbstractRowSet implements SchemaRowSet {
    private final Schema schema;

    EmptyRowSet(Schema schema) {
        this.schema = schema;
    }

    @Override
    protected boolean doHasCurrent() {
        return false;
    }

    @Override
    protected boolean doNext() {
        return false;
    }

    @Override
    protected Object getColumn(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doReset() {
        // no-op
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
