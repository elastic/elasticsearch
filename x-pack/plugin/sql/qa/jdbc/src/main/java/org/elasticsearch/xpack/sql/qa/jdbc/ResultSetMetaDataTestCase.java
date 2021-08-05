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

public abstract class ResultSetMetaDataTestCase extends JdbcIntegrationTestCase {

    private final String[] fieldsNames = new String[] {
        "test_byte",
        "test_integer",
        "test_long",
        "test_short",
        "test_double",
        "test_float",
        "test_keyword",
        "test_boolean",
        "test_date" };

    public void testValidGetObjectCalls() throws IOException, SQLException {
        ResultSetTestCase.createIndex("test");
        ResultSetTestCase.updateMapping("test", builder -> {
            for (String field : fieldsNames) {
                builder.startObject(field).field("type", field.substring(5)).endObject();
            }
        });

        String q = "SELECT test_byte, test_integer, test_long, test_short, test_double, test_float, test_keyword, "
            + "test_boolean, test_date FROM test";
        doWithQuery(q, r -> assertColumnNamesAndLabels(r.getMetaData(), fieldsNames));

        q = "SELECT test_byte AS b, test_integer AS i, test_long AS l, test_short AS s, test_double AS d, test_float AS f, "
            + "test_keyword AS k, test_boolean AS bool, test_date AS dt FROM test";
        doWithQuery(q, r -> assertColumnNamesAndLabels(r.getMetaData(), new String[] { "b", "i", "l", "s", "d", "f", "k", "bool", "dt" }));
    }

    private void doWithQuery(String query, CheckedConsumer<ResultSet, SQLException> consumer) throws SQLException {
        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                try (ResultSet results = statement.executeQuery()) {
                    assertEquals(fieldsNames.length, results.getMetaData().getColumnCount());
                    consumer.accept(results);
                }
            }
        }
    }

    private void assertColumnNamesAndLabels(ResultSetMetaData metaData, String[] names) throws SQLException {
        for (int i = 0; i < fieldsNames.length; i++) {
            assertEquals(names[i], metaData.getColumnName(i + 1));
            assertEquals(names[i], metaData.getColumnLabel(i + 1));
        }
    }
}
