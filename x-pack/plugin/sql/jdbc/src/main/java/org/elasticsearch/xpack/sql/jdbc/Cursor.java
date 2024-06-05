/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

import java.sql.SQLException;
import java.util.List;

interface Cursor {

    List<JdbcColumnInfo> columns();

    default int columnSize() {
        return columns().size();
    }

    boolean next() throws SQLException;

    Object column(int column);

    /**
     * Number of rows that this cursor has pulled back from the
     * server in the current batch.
     */
    int batchSize();

    void close() throws SQLException;

    List<String> warnings();

    void clearWarnings();
}
