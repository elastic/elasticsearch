/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.hasSize;

public class SqlListColumnsResponseTests extends AbstractStreamableXContentTestCase<SqlListColumnsResponse> {

    @Override
    protected SqlListColumnsResponse createTestInstance() {
        int columnCount = between(1, 10);
        boolean jdbcMode = randomBoolean();
        List<MetaColumnInfo> columns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            if (jdbcMode) {
                columns.add(new MetaColumnInfo(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10),
                        randomFrom(JDBCType.values()), randomInt(25), randomInt(20)));
            } else {
                columns.add(new MetaColumnInfo(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10), randomInt(20)));

            }
        }
        return new SqlListColumnsResponse(columns);
    }

    @Override
    protected SqlListColumnsResponse createBlankInstance() {
        return new SqlListColumnsResponse();
    }

    public void testToXContent() throws IOException {
        SqlListColumnsResponse testInstance = createTestInstance();

        XContentBuilder builder = testInstance.toXContent(XContentFactory.jsonBuilder(), EMPTY_PARAMS);
        Map<String, Object> rootMap = XContentHelper.convertToMap(builder.bytes(), false, builder.contentType()).v2();

        logger.info(builder.string());

        if (testInstance.getColumns() != null) {
            List<?> columns = (List<?>) rootMap.get("columns");
            assertThat(columns, hasSize(testInstance.getColumns().size()));
            for (int i = 0; i < columns.size(); i++) {
                Map<?, ?> columnMap = (Map<?, ?>) columns.get(i);
                MetaColumnInfo columnInfo = testInstance.getColumns().get(i);
                assertEquals(columnInfo.table(), columnMap.get("table"));
                assertEquals(columnInfo.name(), columnMap.get("name"));
                assertEquals(columnInfo.esType(), columnMap.get("type"));
                if (columnInfo.jdbcType() == null) {
                    assertNull(columnMap.get("jdbc_type"));
                    assertNull(columnMap.get("size"));
                } else {
                    assertEquals(columnInfo.jdbcType().getVendorTypeNumber(), columnMap.get("jdbc_type"));
                    assertEquals(columnInfo.size(), columnMap.get("size"));
                }
                assertEquals(columnInfo.position(), columnMap.get("position"));
            }
        } else {
            assertNull(rootMap.get("columns"));
        }
    }

    @Override
    protected SqlListColumnsResponse doParseInstance(XContentParser parser) {
        return SqlListColumnsResponse.fromXContent(parser);
    }
}
