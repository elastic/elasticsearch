/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.xpack.ql.type.Schema;
import org.elasticsearch.xpack.sql.session.RowSet;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;

class SchemaDelegatingRowSet implements SchemaRowSet {

    private final Schema schema;
    private final RowSet delegate;

    SchemaDelegatingRowSet(Schema schema, RowSet delegate) {
        this.schema = schema;
        this.delegate = delegate;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public boolean hasCurrentRow() {
        return delegate.hasCurrentRow();
    }

    @Override
    public boolean advanceRow() {
        return delegate.advanceRow();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public Object column(int index) {
        return delegate.column(index);
    }
}
