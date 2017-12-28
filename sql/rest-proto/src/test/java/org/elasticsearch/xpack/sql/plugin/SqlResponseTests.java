/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.plugin.SqlResponse.ColumnInfo;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.hasSize;

public class SqlResponseTests extends AbstractStreamableXContentTestCase<SqlResponse> {

    static String randomStringCursor() {
        return randomBoolean() ? "" : randomAlphaOfLength(10);
    }

    @Override
    protected SqlResponse createTestInstance() {
        return createRandomInstance(randomStringCursor());
    }

    private static SqlResponse createRandomInstance(String cursor) {
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
                    Supplier<Object> value = randomFrom(Arrays.asList(
                            () -> randomAlphaOfLength(10),
                            ESTestCase::randomLong,
                            ESTestCase::randomDouble,
                            () -> null));
                    row.add(value.get());

                }
                rows.add(row);
            }
        }
        return new SqlResponse(cursor, columns, rows);
    }

    @Override
    protected SqlResponse createBlankInstance() {
        return new SqlResponse();
    }

    public void testToXContent() throws IOException {
        SqlResponse testInstance = createTestInstance();

        boolean jdbcEnabled = randomBoolean();
        ToXContent.Params params =
                new ToXContent.MapParams(Collections.singletonMap(SqlResponse.JDBC_ENABLED_PARAM, Boolean.toString(jdbcEnabled)));

        XContentBuilder builder = testInstance.toXContent(XContentFactory.jsonBuilder(), params);
        Map<String, Object> rootMap = XContentHelper.convertToMap(builder.bytes(), false, builder.contentType()).v2();

        logger.info(builder.string());

        if (testInstance.columns() != null) {
            List<?> columns = (List<?>) rootMap.get("columns");
            assertThat(columns, hasSize(testInstance.columns().size()));
            for (int i = 0; i < columns.size(); i++) {
                Map<?, ?> columnMap = (Map<?, ?>) columns.get(i);
                ColumnInfo columnInfo = testInstance.columns().get(i);
                assertEquals(columnInfo.name(), columnMap.get("name"));
                assertEquals(columnInfo.esType(), columnMap.get("type"));
                if (jdbcEnabled) {
                    assertEquals(columnInfo.displaySize(), columnMap.get("display_size"));
                    assertEquals(columnInfo.jdbcType().getVendorTypeNumber(), columnMap.get("jdbc_type"));
                } else {
                    assertNull(columnMap.get("display_size"));
                    assertNull(columnMap.get("jdbc_type"));
                }
            }
        } else {
            assertNull(rootMap.get("columns"));
        }

        List<?> rows = ((List<?>) rootMap.get("rows"));
        assertThat(rows, hasSize(testInstance.rows().size()));
        for (int i = 0; i < rows.size(); i++) {
            List<?> row = (List<?>) rows.get(i);
            assertEquals(row, testInstance.rows().get(i));
        }

        if (testInstance.cursor().equals("") == false) {
            assertEquals(rootMap.get(SqlRequest.CURSOR.getPreferredName()), testInstance.cursor());
        }
    }

    @Override
    protected SqlResponse doParseInstance(XContentParser parser) {
        return SqlResponse.fromXContent(parser);
    }
}
