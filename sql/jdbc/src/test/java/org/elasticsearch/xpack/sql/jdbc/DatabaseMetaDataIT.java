/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.xpack.sql.jdbc.framework.JdbcIntegrationTestCase;
import org.elasticsearch.xpack.sql.jdbc.framework.LocalH2;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.elasticsearch.xpack.sql.jdbc.framework.JdbcAssert.assertResultSets;

/**
 * Tests for our implementation of {@link DatabaseMetaData}.
 */
@AwaitsFix(bugUrl = "https://github.com/elastic/x-pack-elasticsearch/issues/2074")
public class DatabaseMetaDataIT extends JdbcIntegrationTestCase {
    /**
     * We do not support procedures so we return an empty set for
     * {@link DatabaseMetaData#getProcedures(String, String, String)}.
     */
    public void testGetProcedures() throws Exception {
        try (Connection h2 = LocalH2.anonymousDb();
                Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_metadata_get_procedures.sql'");

            ResultSet expected = h2.createStatement().executeQuery("SELECT * FROM mock");
            assertResultSets(expected, es.getMetaData().getProcedures(
                    randomBoolean() ? null : randomAlphaOfLength(5),
                    randomBoolean() ? null : randomAlphaOfLength(5),
                    randomBoolean() ? null : randomAlphaOfLength(5)));
        }
    }

    /**
     * We do not support procedures so we return an empty set for
     * {@link DatabaseMetaData#getProcedureColumns(String, String, String, String)}.
     */
    public void testGetProcedureColumns() throws Exception {
        try (Connection h2 = LocalH2.anonymousDb();
                Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_metadata_get_procedure_columns.sql'");

            ResultSet expected = h2.createStatement().executeQuery("SELECT * FROM mock");
            assertResultSets(expected, es.getMetaData().getProcedureColumns(
                    randomBoolean() ? null : randomAlphaOfLength(5),
                    randomBoolean() ? null : randomAlphaOfLength(5),
                    randomBoolean() ? null : randomAlphaOfLength(5),
                    randomBoolean() ? null : randomAlphaOfLength(5)));
        }
    }

    public void testGetTables() throws Exception {
        index("test", body -> body.field("name", "bob"));

        try (Connection h2 = LocalH2.anonymousDb();
                Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_metadata_get_tables.sql'");

            CheckedSupplier<ResultSet, SQLException> expected = () ->
                h2.createStatement().executeQuery("SELECT '" + clusterName() + "' AS TABLE_CAT, * FROM mock");
            assertResultSets(expected.get(), es.getMetaData().getTables("%", "%", "%", null));
            assertResultSets(expected.get(), es.getMetaData().getTables("%", "%", "te%", null));
            // NOCOMMIT with a wildcard type is broken:
//            assertResultSets(expected.get(), es.getMetaData().getTables("%", "%", "test.d%", null));
        }
    }

    public void testColumns() throws Exception {
        index("test", body -> body.field("name", "bob"));

        try (Connection h2 = LocalH2.anonymousDb();
                Connection es = esJdbc()) {
            h2.createStatement().executeUpdate("RUNSCRIPT FROM 'classpath:/setup_mock_metadata_get_columns.sql'");

            ResultSet expected = h2.createStatement().executeQuery("SELECT '" + clusterName() + "' AS TABLE_CAT, * FROM mock");
            assertResultSets(expected, es.getMetaData().getColumns("%", "%", "%", null));
        }

        // NOCOMMIT add some more tables and more columns and test that.
    }
}
