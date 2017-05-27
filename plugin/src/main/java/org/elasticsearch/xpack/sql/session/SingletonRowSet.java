/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.sql.type.Schema;

class SingletonRowSet extends AbstractRowSetCursor {

    private final Object[] values;

    SingletonRowSet(Schema schema, Object[] values) {
        super(schema, null);
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
}
