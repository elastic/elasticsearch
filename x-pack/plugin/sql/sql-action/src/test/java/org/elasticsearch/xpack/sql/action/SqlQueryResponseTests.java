/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.hasSize;

public class SqlQueryResponseTests extends AbstractStreamableXContentTestCase<SqlQueryResponse> {

    static String randomStringCursor() {
        return randomBoolean() ? "" : randomAlphaOfLength(10);
    }

    @Override
    protected SqlQueryResponse createTestInstance() {
        return createRandomInstance(randomStringCursor());
    }

    public static SqlQueryResponse createRandomInstance(String cursor) {
        int columnCount = between(1, 10);

        List<ColumnInfo> columns = null;
        if (randomBoolean()) {
            columns = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columns.add(new ColumnInfo(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10),
                        randomInt(), randomInt(25)));
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
        return new SqlQueryResponse(cursor, columns, rows);
    }

    @Override
    protected SqlQueryResponse createBlankInstance() {
        return new SqlQueryResponse();
    }

    public void testToXContent() throws IOException {
        SqlQueryResponse testInstance = createTestInstance();

        XContentBuilder builder = testInstance.toXContent(XContentFactory.jsonBuilder(), EMPTY_PARAMS);
        Map<String, Object> rootMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        logger.info(Strings.toString(builder));

        if (testInstance.columns() != null) {
            List<?> columns = (List<?>) rootMap.get("columns");
            assertThat(columns, hasSize(testInstance.columns().size()));
            for (int i = 0; i < columns.size(); i++) {
                Map<?, ?> columnMap = (Map<?, ?>) columns.get(i);
                ColumnInfo columnInfo = testInstance.columns().get(i);
                assertEquals(columnInfo.name(), columnMap.get("name"));
                assertEquals(columnInfo.esType(), columnMap.get("type"));
                assertEquals(columnInfo.displaySize(), columnMap.get("display_size"));
                assertEquals(columnInfo.jdbcType(), columnMap.get("jdbc_type"));
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
            assertEquals(rootMap.get(SqlQueryRequest.CURSOR.getPreferredName()), testInstance.cursor());
        }
    }

    @Override
    protected SqlQueryResponse doParseInstance(XContentParser parser) {
        org.elasticsearch.xpack.sql.proto.SqlQueryResponse response =
            org.elasticsearch.xpack.sql.proto.SqlQueryResponse.fromXContent(parser);
        return new SqlQueryResponse(response.cursor(), response.columns(), response.rows());
    }
}
