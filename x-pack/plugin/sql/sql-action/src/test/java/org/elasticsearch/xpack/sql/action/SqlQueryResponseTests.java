/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Payloads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.sql.action.AbstractSqlQueryRequest.CURSOR;
import static org.elasticsearch.xpack.sql.action.Protocol.ID_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.IS_PARTIAL_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.IS_RUNNING_NAME;
import static org.elasticsearch.xpack.sql.proto.SqlVersion.DATE_NANOS_SUPPORT_VERSION;
import static org.hamcrest.Matchers.hasSize;

public class SqlQueryResponseTests extends AbstractSerializingTestCase<SqlQueryResponse> {

    static String randomStringCursor() {
        return randomBoolean() ? "" : randomAlphaOfLength(10);
    }

    @Override
    protected SqlQueryResponse createXContextTestInstance(XContentType xContentType) {
        SqlTestUtils.assumeXContentJsonOrCbor(xContentType);
        return super.createXContextTestInstance(xContentType);
    }

    @Override
    protected SqlQueryResponse createTestInstance() {
        return createRandomInstance(
            randomStringCursor(),
            randomFrom(Mode.values()),
            randomBoolean(),
            rarely() ? null : randomAlphaOfLength(100),
            randomBoolean(),
            randomBoolean()
        );
    }

    @Override
    protected Writeable.Reader<SqlQueryResponse> instanceReader() {
        return SqlQueryResponse::new;
    }

    public static SqlQueryResponse createRandomInstance(
        String cursor,
        Mode mode,
        boolean columnar,
        String asyncExecutionId,
        boolean isPartial,
        boolean isRunning
    ) {
        int columnCount = between(1, 10);

        List<ColumnInfo> columns = null;
        if (randomBoolean()) {
            columns = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columns.add(
                    new ColumnInfo(
                        randomAlphaOfLength(10),
                        randomAlphaOfLength(10),
                        randomAlphaOfLength(10),
                        randomBoolean() ? null : randomInt(25)
                    )
                );
            }
        }

        List<List<Object>> rows;
        if (randomBoolean()) {
            rows = Collections.emptyList();
        } else {
            int rowCount = between(1, 10);
            if (columnar && columns != null) {
                int temp = rowCount;
                rowCount = columnCount;
                columnCount = temp;
            }

            rows = new ArrayList<>(rowCount);
            for (int r = 0; r < rowCount; r++) {
                List<Object> row = new ArrayList<>(rowCount);
                for (int c = 0; c < columnCount; c++) {
                    Supplier<Object> value = randomFrom(
                        Arrays.asList(() -> randomAlphaOfLength(10), ESTestCase::randomLong, ESTestCase::randomDouble, () -> null)
                    );
                    row.add(value.get());
                }
                rows.add(row);
            }
        }
        return new SqlQueryResponse(cursor, mode, DATE_NANOS_SUPPORT_VERSION, false, columns, rows, asyncExecutionId, isPartial, isRunning);
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
            }
        } else {
            assertNull(rootMap.get("columns"));
        }

        List<?> rows;
        if (testInstance.columnar()) {
            rows = ((List<?>) rootMap.get("values"));
        } else {
            rows = ((List<?>) rootMap.get("rows"));
        }
        assertNotNull(rows);
        assertThat(rows, hasSize(testInstance.rows().size()));
        for (int i = 0; i < rows.size(); i++) {
            List<?> row = (List<?>) rows.get(i);
            assertEquals(row, testInstance.rows().get(i));
        }

        if (testInstance.cursor().equals("") == false) {
            assertEquals(rootMap.get(CURSOR.getPreferredName()), testInstance.cursor());
        }

        if (Strings.hasText(testInstance.id())) {
            assertEquals(testInstance.id(), rootMap.get(ID_NAME));
            assertEquals(testInstance.isPartial(), rootMap.get(IS_PARTIAL_NAME));
            assertEquals(testInstance.isRunning(), rootMap.get(IS_RUNNING_NAME));
        }
    }

    @Override
    protected SqlQueryResponse doParseInstance(XContentParser parser) throws IOException {
        org.elasticsearch.xpack.sql.proto.SqlQueryResponse protoResponse = SqlTestUtils.fromXContentParser(
            parser,
            Payloads::parseQueryResponse
        );

        return new SqlQueryResponse(
            protoResponse.cursor(),
            Mode.JDBC,
            DATE_NANOS_SUPPORT_VERSION,
            false,
            protoResponse.columns(),
            protoResponse.rows(),
            protoResponse.id(),
            protoResponse.isPartial(),
            protoResponse.isRunning()
        );
    }
}
