/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.core.CheckedConsumer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.JDBC_DRIVER_VERSION;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.UNSIGNED_LONG_TYPE_NAME;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.isUnsignedLongSupported;

public abstract class ResultSetMetaDataTestCase extends JdbcIntegrationTestCase {

    private static final List<String> FIELDS_NAMES = List.of(
        "test_byte",
        "test_integer",
        "test_long",
        "test_short",
        "test_double",
        "test_float",
        "test_keyword",
        "test_boolean",
        "test_date"
    );
    private static final String UNSIGNED_LONG_FIELD = "test_" + UNSIGNED_LONG_TYPE_NAME.toLowerCase(Locale.ROOT);

    private static void createMappedIndex(List<String> fieldsNames) throws IOException {
        ResultSetTestCase.createIndex("test");
        ResultSetTestCase.updateMapping("test", builder -> {
            for (String field : fieldsNames) {
                builder.startObject(field).field("type", field.substring(5)).endObject();
            }
        });
    }

    public void testValidGetObjectCalls() throws IOException, SQLException {
        doTestValidGetObjectCalls(FIELDS_NAMES);
    }

    public void testValidGetObjectCallsWithUnsignedLong() throws IOException, SQLException {
        assumeTrue("Driver version [" + JDBC_DRIVER_VERSION + "] doesn't support UNSIGNED_LONGs", isUnsignedLongSupported());

        doTestValidGetObjectCalls(singletonList(UNSIGNED_LONG_FIELD));
    }

    private void doTestValidGetObjectCalls(List<String> fieldsNames) throws IOException, SQLException {
        createMappedIndex(fieldsNames);

        String q = "SELECT " + String.join(", ", fieldsNames) + " FROM test";
        doWithQuery(q, r -> assertColumnNamesAndLabels(r.getMetaData(), fieldsNames));

        String selectedFields = fieldsNames.stream().map(x -> x + " AS " + x.replace("_", "")).collect(Collectors.joining(", "));
        q = "SELECT " + selectedFields + " FROM test";
        doWithQuery(
            q,
            r -> assertColumnNamesAndLabels(r.getMetaData(), fieldsNames.stream().map(x -> x.replace("_", "")).collect(Collectors.toList()))
        );
    }

    public void testUnsignedLongConditionallyReturnedOnStarExpansion() throws IOException, SQLException {
        List<String> fieldsNames = new ArrayList<>(FIELDS_NAMES);
        fieldsNames.add(UNSIGNED_LONG_FIELD);
        createMappedIndex(fieldsNames);

        String query = "SELECT * FROM test";
        doWithQuery(query, r -> {
            List<String> columnTypeNames = new ArrayList<>(fieldsNames.size());
            for (int i = 0; i < r.getMetaData().getColumnCount(); i++) {
                columnTypeNames.add(r.getMetaData().getColumnTypeName(i + 1).toLowerCase(Locale.ROOT));
            }
            // the assert executes only if UL is supported; the failing case would be triggered earlier in the driver already
            assertEquals(isUnsignedLongSupported(), columnTypeNames.contains(UNSIGNED_LONG_TYPE_NAME.toLowerCase(Locale.ROOT)));
        });
    }

    private void doWithQuery(String query, CheckedConsumer<ResultSet, SQLException> consumer) throws SQLException {
        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                try (ResultSet results = statement.executeQuery()) {
                    consumer.accept(results);
                }
            }
        }
    }

    private void assertColumnNamesAndLabels(ResultSetMetaData metaData, List<String> names) throws SQLException {
        assertEquals(names.size(), metaData.getColumnCount());
        for (int i = 0; i < names.size(); i++) {
            assertEquals(names.get(i), metaData.getColumnName(i + 1));
            assertEquals(names.get(i), metaData.getColumnLabel(i + 1));
        }
    }
}
