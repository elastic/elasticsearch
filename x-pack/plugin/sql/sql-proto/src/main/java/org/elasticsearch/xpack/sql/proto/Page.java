/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import java.lang.reflect.Array;
import java.util.List;

// Stores a page of data in a columnar-format since:
// * the structure does not change
// * array allocation can be quite efficient
// * array can be reallocated (as the pages have the same size)

// c1 c1 c1
// c2 c2 c2 ...
public class Page {

    private static final Page EMPTY = new Page(0, new Object[0][0]);
    // logical limit
    private int rows;

    private final Object[][] data;

    private Page(int rows, Object[][] data) {
        this.rows = rows;
        this.data = data;
    }

    void resize(int newLength) {
        if (data.length < 1) {
            return;
        }

        // resize only when needed
        // the array is kept around so check its length not the logical limit
        if (newLength > data[0].length) {
            for (int i = 0; i < data.length; i++) {
                data[i] = (Object[]) Array.newInstance(data[i].getClass().getComponentType(), newLength);
            }
        }
        rows = newLength;
    }

    public int rows() {
        return rows;
    }

    Object[] column(int index) {
        Utils.checkIndex(index, data.length);
        return data[index];
    }

    public Object entry(int row, int column) {
        Utils.checkIndex(row, rows);
        return column(column)[row];
    }

    static Page of(List<Class<?>> types, int dataSize) {
        if (types == null || types.isEmpty()) {
            return EMPTY;
        }

        Object[][] data = new Object[types.size()][];

        for (int i = 0; i < types.size(); i++) {
            data[i] = (Object[]) Array.newInstance(types.get(i), dataSize);
        }

        return new Page(dataSize, data);
    }
}