/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.server.jdbc;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.ResultPage;
import org.elasticsearch.xpack.sql.session.RowSet;
import org.joda.time.ReadableInstant;

import java.io.DataOutput;
import java.io.IOException;
import java.sql.JDBCType;

/**
 * Adapts {@link RowSet} into a {@link ResultPage} so it can be serialized.
 * Note that we are careful not to read the {@linkplain RowSet} more then
 * once.
 */
public class RowSetCursorResultPage extends ResultPage {
    private final RowSet rowSet;

    public RowSetCursorResultPage(RowSet rowSet) {
        this.rowSet = rowSet;
    }

    public void write(DataOutput out) throws IOException {
        int rows = rowSet.size();
        out.writeInt(rows);
        if (rows == 0) {
            return;
        }
        do {
            for (int column = 0; column < rowSet.rowSize(); column++) {
                JDBCType columnType = rowSet.schema().types().get(column).sqlType();
                Object value = rowSet.column(column);
                if (columnType == JDBCType.TIMESTAMP && value instanceof ReadableInstant) {
                    // TODO it feels like there should be a better way to do this
                    value = ((ReadableInstant) value).getMillis();
                }
                writeValue(out, value, columnType);
            }
        } while (rowSet.advanceRow());
    }
}
