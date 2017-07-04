/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.client;

import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcException;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcUtils;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfo;

import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

// Stores a page of data in a columnar-format since:
// * the structure does not change
// * array allocation can be quite efficient
// * array can be reallocated (especially since the pages have the same size)

// c1 c1 c1
// c2 c2 c2 ...
public class Page {

    // logical limit
    private int rows;

    private final List<ColumnInfo> columnInfo;
    private final Object[][] data;

    private Page(int rows, List<ColumnInfo> columnInfo, Object[][] data) {
        this.rows = rows;
        this.columnInfo = columnInfo;
        this.data = data;
    }

    void resize(int newLength) {
        // resize only when needed
        // the array is kept around so check its length not the logical limit
        if (newLength > data[0].length) {
            for (int i = 0; i < data.length; i++) {
                data[i] = (Object[]) Array.newInstance(data[i].getClass().getComponentType(), newLength);
            }
        }
        rows = newLength;
    }

    int rows() {
        return rows;
    }

    List<ColumnInfo> columnInfo() {
        return columnInfo;
    }

    Object[] column(int index) {
        if (index < 0 || index >= data.length) {
            throw new JdbcException("Invalid column %d (max is %d)", index, data.length - 1);
        }

        return data[index];
    }

    Object entry(int row, int column) {
        if (row < 0 || row >= rows) {
            throw new JdbcException("Invalid row %d (max is %d)", row, rows - 1);
        }
        return column(column)[row];
    }

    static Page of(List<ColumnInfo> columnInfo, int dataSize) {
        Object[][] data = new Object[columnInfo.size()][];

        for (int i = 0; i < columnInfo.size(); i++) {
            Class<?> types = JdbcUtils.classOf(columnInfo.get(i).type);
            if (types == Timestamp.class || types == Date.class || types == Time.class) {
                types = Long.class;
            }
            data[i] = (Object[]) Array.newInstance(types, dataSize);
        }

        return new Page(dataSize, columnInfo, data);
    }
}