/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.sql.type.Schema;

//TODO is it worth keeping this when we have ListRowSet?
class SingletonRowSet extends AbstractRowSet implements SchemaRowSet {

    private final Schema schema;
    private final Object[] values;

    SingletonRowSet(Schema schema, Object[] values) {
        this.schema = schema;
        this.values = values;
    }

    @Override
    protected boolean doHasCurrent() {
        return true;
    }

    @Override
    protected boolean doNext() {
        return false;
    }

    @Override
    protected Object getColumn(int index) {
        return values[index];
    }

    @Override
    protected void doReset() {
        // no-op
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
