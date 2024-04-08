/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Locale;

import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcAssert.assertResultSets;

public class ShowTablesTestCase extends JdbcIntegrationTestCase {
    public void testShowTablesWithoutAnyIndexes() throws Exception {
        try (Connection h2 = LocalH2.anonymousDb(); Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_show_tables.sql'");

            ResultSet expected = h2.createStatement().executeQuery("SELECT * FROM mock");
            assertResultSets(expected, es.createStatement().executeQuery("SHOW TABLES"));
        }
    }

    public void testShowTablesWithManyIndices() throws Exception {
        try (Connection h2 = LocalH2.anonymousDb(); Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_show_tables.sql'");
            int indices = between(2, 20);
            for (int i = 0; i < indices; i++) {
                String index = String.format(Locale.ROOT, "test%02d", i);
                index(index, builder -> builder.field("name", "bob"));
                h2.createStatement().executeUpdate("INSERT INTO mock VALUES ('javaRestTest', '" + index + "', 'TABLE', 'INDEX');");
            }

            ResultSet expected = h2.createStatement().executeQuery("SELECT * FROM mock ORDER BY name");
            assertResultSets(expected, es.createStatement().executeQuery("SHOW TABLES"));
        }
    }

    public void testEmptyIndex() throws Exception {
        DataLoader.createEmptyIndex(client(), "test_empty");
        DataLoader.createEmptyIndex(client(), "test_empty_again");

        try (Connection h2 = LocalH2.anonymousDb(); Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_show_tables.sql'");
            h2.createStatement().executeUpdate("INSERT INTO mock VALUES ('javaRestTest', 'test_empty', 'TABLE', 'INDEX');");
            h2.createStatement().executeUpdate("INSERT INTO mock VALUES ('javaRestTest', 'test_empty_again', 'TABLE', 'INDEX');");

            ResultSet expected = h2.createStatement().executeQuery("SELECT * FROM mock");
            assertResultSets(expected, es.createStatement().executeQuery("SHOW TABLES"));
        }
    }
}
