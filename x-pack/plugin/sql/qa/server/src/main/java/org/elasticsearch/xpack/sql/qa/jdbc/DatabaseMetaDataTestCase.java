/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.common.CheckedSupplier;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcAssert.assertResultSets;

/**
 * Tests for our implementation of {@link DatabaseMetaData}.
 */
public class DatabaseMetaDataTestCase extends JdbcIntegrationTestCase {
    /**
     * We do not support procedures so we return an empty set for
     * {@link DatabaseMetaData#getProcedures(String, String, String)}.
     */
    public void testGetProcedures() throws Exception {
        try (Connection h2 = LocalH2.anonymousDb(); Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_metadata_get_procedures.sql'");

            ResultSet expected = h2.createStatement().executeQuery("SELECT * FROM mock");
            assertResultSets(
                expected,
                es.getMetaData()
                    .getProcedures(
                        randomBoolean() ? null : randomAlphaOfLength(5),
                        randomBoolean() ? null : randomAlphaOfLength(5),
                        randomBoolean() ? null : randomAlphaOfLength(5)
                    )
            );
        }
    }

    /**
     * We do not support procedures so we return an empty set for
     * {@link DatabaseMetaData#getProcedureColumns(String, String, String, String)}.
     */
    public void testGetProcedureColumns() throws Exception {
        try (Connection h2 = LocalH2.anonymousDb(); Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_metadata_get_procedure_columns.sql'");

            ResultSet expected = h2.createStatement().executeQuery("SELECT * FROM mock");
            assertResultSets(
                expected,
                es.getMetaData()
                    .getProcedureColumns(
                        randomBoolean() ? null : randomAlphaOfLength(5),
                        randomBoolean() ? null : randomAlphaOfLength(5),
                        randomBoolean() ? null : randomAlphaOfLength(5),
                        randomBoolean() ? null : randomAlphaOfLength(5)
                    )
            );
        }
    }

    public void testGetTables() throws Exception {
        index("test1", body -> body.field("name", "bob"));
        index("test2", body -> body.field("name", "bob"));

        try (Connection h2 = LocalH2.anonymousDb(); Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_metadata_get_tables.sql'");

            CheckedSupplier<ResultSet, SQLException> all = () -> h2.createStatement()
                .executeQuery("SELECT '" + clusterName() + "' AS TABLE_CAT, * FROM mock");
            assertResultSets(all.get(), es.getMetaData().getTables("%", "%", "%", null));
            assertResultSets(all.get(), es.getMetaData().getTables("%", "%", "te%", null));
            assertResultSets(
                h2.createStatement().executeQuery("SELECT '" + clusterName() + "' AS TABLE_CAT, * FROM mock WHERE TABLE_NAME = 'test1'"),
                es.getMetaData().getTables("%", "%", "test1", null)
            );
        }
    }

    public void testGetTablesForEmptyIndices() throws Exception {
        DataLoader.createEmptyIndex(client(), "test_empty");
        DataLoader.createEmptyIndex(client(), "test_empty_again");

        try (Connection h2 = LocalH2.anonymousDb(); Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_metadata_get_tables_empty.sql'");

            CheckedSupplier<ResultSet, SQLException> all = () -> h2.createStatement()
                .executeQuery("SELECT '" + clusterName() + "' AS TABLE_CAT, * FROM mock");
            assertResultSets(all.get(), es.getMetaData().getTables("%", "%", "%", null));
            assertResultSets(all.get(), es.getMetaData().getTables("%", "%", "te%", null));
            assertResultSets(
                h2.createStatement()
                    .executeQuery("SELECT '" + clusterName() + "' AS TABLE_CAT, * FROM mock WHERE TABLE_NAME = 'test_empty'"),
                es.getMetaData().getTables("%", "%", "test_empty", null)
            );
        }
    }

    public void testGetTypeOfTables() throws Exception {
        index("test1", body -> body.field("name", "bob"));
        index("test2", body -> body.field("name", "bob"));

        try (Connection h2 = LocalH2.anonymousDb(); Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_metadata_get_types_of_table.sql'");

            CheckedSupplier<ResultSet, SQLException> all = () -> h2.createStatement()
                .executeQuery("SELECT '" + clusterName() + "' AS TABLE_CAT, * FROM mock");
            assertResultSets(all.get(), es.getMetaData().getTables("%", "%", "%", new String[] { "TABLE" }));
            assertResultSets(
                h2.createStatement().executeQuery("SELECT '" + clusterName() + "' AS TABLE_CAT, * FROM mock WHERE TABLE_NAME = 'test1'"),
                es.getMetaData().getTables("%", "%", "test1", new String[] { "TABLE" })
            );
        }
    }

    public void testGetTableTypes() throws Exception {
        index("test1", body -> body.field("name", "bob"));
        index("test2", body -> body.field("name", "bob"));

        try (Connection h2 = LocalH2.anonymousDb(); Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_metadata_get_table_types.sql'");
            assertResultSets(h2.createStatement().executeQuery("SELECT * FROM mock"), es.getMetaData().getTableTypes());
        }
    }

    public void testGetCatalogs() throws Exception {
        try (Connection h2 = LocalH2.anonymousDb(); Connection es = esJdbc()) {
            assertResultSets(
                h2.createStatement().executeQuery("SELECT '" + clusterName() + "' AS TABLE_CAT"),
                es.getMetaData().getCatalogs()
            );
        }
    }

    public void testColumns() throws Exception {
        index("test1", body -> body.field("name", "bob"));
        index("test2", body -> {
            body.field("number", 7);
            body.field("date", "2017-01-01T01:01:01Z");
            body.field("float", 42.0);
        });

        try (Connection h2 = LocalH2.anonymousDb(); Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_metadata_get_columns.sql'");

            ResultSet expected = h2.createStatement().executeQuery("SELECT '" + clusterName() + "' AS TABLE_CAT, * FROM mock");
            assertResultSets(expected, es.getMetaData().getColumns(null, "%", "%", null));
        }
    }

    public void testColumnsForEmptyTable() throws Exception {
        try (Connection h2 = LocalH2.anonymousDb(); Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_metadata_get_columns_empty.sql'");

            ResultSet expected = h2.createStatement().executeQuery("SELECT '" + clusterName() + "' AS TABLE_CAT, * FROM mock");
            assertResultSets(expected, es.getMetaData().getColumns(null, "%", "%", null));
        }
    }
}
