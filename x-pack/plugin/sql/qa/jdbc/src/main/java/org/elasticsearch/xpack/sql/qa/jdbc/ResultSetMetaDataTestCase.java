/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.common.CheckedConsumer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.isUnsignedLongSupported;

public abstract class ResultSetMetaDataTestCase extends JdbcIntegrationTestCase {

    private static final List<String> fieldsNames = new ArrayList<>();

    static {
        fieldsNames.addAll(List.of(
            "test_byte",
            "test_integer",
            "test_long",
            "test_short",
            "test_double",
            "test_float",
            "test_keyword",
            "test_boolean",
            "test_date"
        ));
        if (isUnsignedLongSupported()) {
            fieldsNames.add("test_unsigned_long");
        }
    }

    public void testValidGetObjectCalls() throws IOException, SQLException {
        ResultSetTestCase.createIndex("test");
        ResultSetTestCase.updateMapping("test", builder -> {
            for (String field : fieldsNames) {
                builder.startObject(field).field("type", field.substring(5)).endObject();
            }
        });

        String q = "SELECT " + String.join(", ", fieldsNames) + " FROM test";
        doWithQuery(q, r -> assertColumnNamesAndLabels(r.getMetaData(), fieldsNames));


        q = "SELECT " + fieldsNames.stream().map(x -> x + " AS " + x.replace("_", "")).collect(Collectors.joining(", ")) + " FROM test";
        doWithQuery(q, r -> assertColumnNamesAndLabels(r.getMetaData(), fieldsNames.stream()
            .map(x -> x.replace("_", "")).collect(Collectors.toList())));
    }

    private void doWithQuery(String query, CheckedConsumer<ResultSet, SQLException> consumer) throws SQLException {
        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                try (ResultSet results = statement.executeQuery()) {
                    assertEquals(fieldsNames.size(), results.getMetaData().getColumnCount());
                    consumer.accept(results);
                }
            }
        }
    }

    private void assertColumnNamesAndLabels(ResultSetMetaData metaData, List<String> names) throws SQLException {
        for (int i = 0; i < fieldsNames.size(); i++) {
            assertEquals(names.get(i), metaData.getColumnName(i + 1));
            assertEquals(names.get(i), metaData.getColumnLabel(i + 1));
        }
    }
}
