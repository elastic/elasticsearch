/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.sql.execution.search.ScrollCursorTests;
import org.elasticsearch.xpack.sql.plugin.SqlPlugin;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponse.ColumnInfo;
import org.elasticsearch.xpack.sql.session.Cursor;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlResponseTests extends AbstractStreamableTestCase<SqlResponse> {
    static Cursor randomCursor() {
        return randomBoolean() ? Cursor.EMPTY : ScrollCursorTests.randomScrollCursor();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(SqlPlugin.getNamedWriteables());
    }

    @Override
    protected SqlResponse createTestInstance() {
        int columnCount = between(1, 10);

        List<ColumnInfo> columns = null;
        if (randomBoolean()) {
            columns = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columns.add(new ColumnInfo(randomAlphaOfLength(10), randomAlphaOfLength(10), randomFrom(JDBCType.values()), randomInt(25)));
            }
        }

        List<List<Object>> rows;
        if (randomBoolean()) {
            rows = Collections.emptyList();
        } else {
            int rowCount = between(1, 10);
            rows = new ArrayList<>(rowCount);
            for (int r = 0; r < rowCount; r++) {
                List<Object> row = new ArrayList<>(rowCount);
                for (int c = 0; c < columnCount; c++) {
                    row.add(randomBoolean() ? randomAlphaOfLength(10) : randomInt());
                }
                rows.add(row);
            }
        }

        return new SqlResponse(randomCursor(), randomNonNegativeLong(), columnCount, columns, rows);
    }

    @Override
    protected SqlResponse createBlankInstance() {
        return new SqlResponse();
    }

    // NOCOMMIT add tests for toXcontent
}
