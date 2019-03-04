/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.ListRowSet;
import org.elasticsearch.xpack.sql.type.Schema;

import java.util.List;

class PagingListRowSet extends ListRowSet {

    private final int pageSize;
    private final int columnCount;
    private final Cursor cursor;

    PagingListRowSet(List<List<?>> list, int columnCount, int pageSize) {
        this(Schema.EMPTY, list, columnCount, pageSize);
    }

    PagingListRowSet(Schema schema, List<List<?>> list, int columnCount, int pageSize) {
        super(schema, list);
        this.columnCount = columnCount;
        this.pageSize = Math.min(pageSize, list.size());
        this.cursor = list.size() > pageSize ? new PagingListCursor(list, columnCount, pageSize) : Cursor.EMPTY;
    }

    @Override
    public int size() {
        return pageSize;
    }

    @Override
    public int columnCount() {
        return columnCount;
    }

    @Override
    public Cursor nextPageCursor() {
        return cursor;
    }
}
