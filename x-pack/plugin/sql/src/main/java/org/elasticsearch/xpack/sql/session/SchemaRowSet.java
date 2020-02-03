/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.ql.type.Schema;

/**
 * A {@linkplain RowSet} with the {@link Schema} for the results
 * attached.
 */
public interface SchemaRowSet extends RowSet {
    /**
     * Schema for the results.
     */
    Schema schema();

    @Override
    default int columnCount() {
        return schema().size();
    }
}
