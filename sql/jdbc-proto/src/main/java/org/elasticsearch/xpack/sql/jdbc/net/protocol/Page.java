/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.JDBCType;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.jdbc.net.protocol.ProtoUtils.classOf;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.ProtoUtils.readValue;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.ProtoUtils.writeValue;
/**
 * Stores a page of data in a columnar format.
 */
public class Page implements Payload {
    private final List<ColumnInfo> columnInfo;

    /**
     * The actual data, one array per column.
     */
    private final Object[][] data;

    /**
     * The number of rows in this page. The {@link #data} arrays may be larger
     * but data after the end of the arrays is garbage.
     */
    private int rows;

    private int maxRows;

    /**
     * Build empty, call {@link #read(DataInput)} after to fill it.
     */
    Page(List<ColumnInfo> columnInfo) {
        this.columnInfo = columnInfo;
        data = new Object[columnInfo.size()][];
    }

    /**
     * Build with a particular set of rows. Use this for testing.
     */
    Page(List<ColumnInfo> columnInfo, Object[][] rows) {
        this(columnInfo);
        makeRoomFor(rows.length);
        this.rows = rows.length;
        for (int row = 0; row < rows.length; row++) {
            if (columnInfo.size() != rows[row].length) {
                throw new IllegalArgumentException("Column count mismatch. Got [" + columnInfo.size()
                        + "] ColumnInfos but [" + rows.length + "] columns on the [" + row + "] row.");
            }
        }
        for (int column = 0; column < columnInfo.size(); column++) {
            for (int row = 0; row < rows.length; row++) {
                data[column][row] = rows[row][column];
            }
        }
    }

    public int rows() {
        return rows;
    }

    public List<ColumnInfo> columnInfo() {
        return columnInfo;
    }

    Object[] column(int index) {
        if (index < 0 || index >= data.length) {
            // NOCOMMIT this was once JdbcException. Make sure it isn't now busted 
            throw new IllegalArgumentException("Invalid column [" + index + "] (max is [" + (data.length - 1) + "])");
        }

        return data[index];
    }

    public Object entry(int row, int column) {
        if (row < 0 || row >= rows) {
            // NOCOMMIT this was once JdbcException. Make sure it isn't now busted
            throw new IllegalArgumentException("Invalid row [" + row + "] (max is [" + (rows -1) + "])");
        }
        return column(column)[row];
    }

    /**
     * Read a value from the stream
     */
    public void read(DataInput in) throws IOException {
        int rows = in.readInt();
        // this.rows may be less than the number of rows we have space for
        if (rows > maxRows) {
            makeRoomFor(rows);
        }
        this.rows = rows;

        for (int row = 0; row < rows; row++) {
            for (int column = 0; column < columnInfo.size(); column++) {
                data[column][row] = readValue(in, columnInfo.get(column).type);
            }
        }
    }

    public void write(DataOutput out) throws IOException {
        int rows = rows();
        out.writeInt(rows);
        for (int row = 0; row < rows; row++) {
            for (int column = 0; column < columnInfo.size(); column++) {
                JDBCType columnType = columnInfo.get(column).type;
                writeValue(out, entry(row, column), columnType);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        for (int row = 0; row < rows(); row++) {
            for (int column = 0; column < columnInfo.size(); column++) {
                if (column > 0) {
                    b.append(", ");
                }
                b.append(entry(row, column));
            }
            b.append('\n');
        }
        return b.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj instanceof Page == false) {
            return false;
        }
        Page other = (Page) obj;
        if (rows != other.rows) {
            return false;
        }
        if (false == columnInfo.equals(other.columnInfo)) {
            return false;
        }
        for (int row = 0; row < rows(); row++) {
            for (int column = 0; column < columnInfo.size(); column++) {
                if (false == Objects.equals(entry(row, column), other.entry(row, column))) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(rows(), columnInfo.size());
        for (int row = 0; row < rows(); row++) {
            for (int column = 0; column < columnInfo.size(); column++) {
                Object entry = entry(row, column);
                result = result * 31 + (entry == null ? 0 : entry.hashCode());
            }
        }
        return result;
    }


    private void makeRoomFor(int rows) {
        maxRows = rows;
        for (int i = 0; i < columnInfo.size(); i++) {
            Class<?> type = classOf(columnInfo.get(i).type);
            data[i] = (Object[]) Array.newInstance(type, rows);
        }
    }
}