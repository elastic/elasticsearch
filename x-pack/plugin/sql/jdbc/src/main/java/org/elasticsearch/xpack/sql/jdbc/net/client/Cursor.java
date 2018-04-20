/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.client;

import java.sql.SQLException;
import java.util.List;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfo;

public interface Cursor {

    List<ColumnInfo> columns();

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
}
