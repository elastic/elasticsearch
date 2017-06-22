/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.sql.type.Schema;

class EmptyRowSetCursor extends AbstractRowSetCursor {

    EmptyRowSetCursor(Schema schema) {
        super(schema, null);
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
}
