/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for our implementation of {@link DatabaseMetaData}.
 */
public class DatabaseMetaDataIT extends JdbcIntegrationTestCase {
    /**
     * We do not support procedures so we return an empty set for {@link DatabaseMetaData#getProcedures(String, String, String)}.
     */
    public void testMetadataGetProcedures() throws Exception {
        j.consume(c -> {
            DatabaseMetaData metaData = c.getMetaData();
            ResultSet results = metaData.getProcedures(
                    randomBoolean() ? null : randomAlphaOfLength(5),
                    randomBoolean() ? null : randomAlphaOfLength(5),
                    randomBoolean() ? null : randomAlphaOfLength(5));
            ResultSetMetaData meta = results.getMetaData();
            int i = 1;
            assertColumn("PROCEDURE_CAT", "VARCHAR", meta, i++);
            assertColumn("PROCEDURE_SCHEM", "VARCHAR", meta, i++);
            assertColumn("PROCEDURE_NAME", "VARCHAR", meta, i++);
            assertColumn("NUM_INPUT_PARAMS", "INTEGER", meta, i++);
            assertColumn("NUM_OUTPUT_PARAMS", "INTEGER", meta, i++);
            assertColumn("NUM_RESULT_SETS", "INTEGER", meta, i++);
            assertColumn("REMARKS", "VARCHAR", meta, i++);
            assertColumn("PROCEDURE_TYPE", "SMALLINT", meta, i++);
            assertColumn("SPECIFIC_NAME", "VARCHAR", meta, i++);
            assertEquals(i - 1, meta.getColumnCount());

            assertFalse(results.next());
        });
    }

    public void testMetadataGetProcedureColumns() throws Exception {
        j.consume(c -> {
            DatabaseMetaData metaData = c.getMetaData();
            ResultSet results = metaData.getProcedureColumns(
                    randomBoolean() ? null : randomAlphaOfLength(5),
                    randomBoolean() ? null : randomAlphaOfLength(5),
                    randomBoolean() ? null : randomAlphaOfLength(5),
                    randomBoolean() ? null : randomAlphaOfLength(5));
            ResultSetMetaData meta = results.getMetaData();
            int i = 1;
            assertColumn("PROCEDURE_CAT", "VARCHAR", meta, i++);
            assertColumn("PROCEDURE_SCHEM", "VARCHAR", meta, i++);
            assertColumn("PROCEDURE_NAME", "VARCHAR", meta, i++);
            assertColumn("COLUMN_NAME", "VARCHAR", meta, i++);
            assertColumn("COLUMN_TYPE", "SMALLINT", meta, i++);
            assertColumn("DATA_TYPE", "INTEGER", meta, i++);
            assertColumn("TYPE_NAME", "VARCHAR", meta, i++);
            assertColumn("PRECISION", "INTEGER", meta, i++);
            assertColumn("LENGTH", "INTEGER", meta, i++);
            assertColumn("SCALE", "SMALLINT", meta, i++);
            assertColumn("RADIX", "SMALLINT", meta, i++);
            assertColumn("NULLABLE", "SMALLINT", meta, i++);
            assertColumn("REMARKS", "VARCHAR", meta, i++);
            assertColumn("COLUMN_DEF", "VARCHAR", meta, i++);
            assertColumn("SQL_DATA_TYPE", "INTEGER", meta, i++);
            assertColumn("SQL_DATETIME_SUB", "INTEGER", meta, i++);
            assertColumn("CHAR_OCTET_LENGTH", "INTEGER", meta, i++);
            assertColumn("ORDINAL_POSITION", "INTEGER", meta, i++);
            assertColumn("IS_NULLABLE", "VARCHAR", meta, i++);
            assertColumn("SPECIFIC_NAME", "VARCHAR", meta, i++);
            assertEquals(i - 1, meta.getColumnCount());

            assertFalse(results.next());
        });
    }

    public void testMetadataGetTables() throws Exception {
        index("test", body -> body.field("name", "bob"));
        j.consume(c -> {
            DatabaseMetaData metaData = c.getMetaData();
            ResultSet results = metaData.getTables("%", "%", "%", null);
            ResultSetMetaData meta = results.getMetaData();
            int i = 1;
            assertColumn("TABLE_CAT", "VARCHAR", meta, i++);
            assertColumn("TABLE_SCHEM", "VARCHAR", meta, i++);
            assertColumn("TABLE_NAME", "VARCHAR", meta, i++);
            assertColumn("TABLE_TYPE", "VARCHAR", meta, i++);
            assertColumn("REMARKS", "VARCHAR", meta, i++);
            assertColumn("TYPE_CAT", "VARCHAR", meta, i++);
            assertColumn("TYPE_SCHEM", "VARCHAR", meta, i++);
            assertColumn("TYPE_NAME", "VARCHAR", meta, i++);
            assertColumn("SELF_REFERENCING_COL_NAME", "VARCHAR", meta, i++);
            assertColumn("REF_GENERATION", "VARCHAR", meta, i++);
            assertEquals(i - 1, meta.getColumnCount());

            assertTrue(results.next());
            i = 1;
            assertThat(results.getString(i++), startsWith("x-pack-elasticsearch_sql-clients_jdbc_"));
            assertEquals("", results.getString(i++));
            assertEquals("test.doc", results.getString(i++));
            assertEquals("TABLE", results.getString(i++));
            assertEquals("", results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertFalse(results.next());

            results = metaData.getTables("%", "%", "te%", null);
            assertTrue(results.next());
            assertEquals("test.doc", results.getString(3));
            assertFalse(results.next());

            results = metaData.getTables("%", "%", "test.d%", null);
            assertTrue(results.next());
            assertEquals("test.doc", results.getString(3));
            assertFalse(results.next());
        });
    }

    public void testMetadataColumns() throws Exception {
        index("test", body -> body.field("name", "bob"));
        j.consume(c -> {
            DatabaseMetaData metaData = c.getMetaData();
            ResultSet results = metaData.getColumns("%", "%", "%", null);
            ResultSetMetaData meta = results.getMetaData();
            int i = 1;
            assertColumn("TABLE_CAT", "VARCHAR", meta, i++);
            assertColumn("TABLE_SCHEM", "VARCHAR", meta, i++);
            assertColumn("TABLE_NAME", "VARCHAR", meta, i++);
            assertColumn("COLUMN_NAME", "VARCHAR", meta, i++);
            assertColumn("DATA_TYPE", "INTEGER", meta, i++);
            assertColumn("TYPE_NAME", "VARCHAR", meta, i++);
            assertColumn("COLUMN_SIZE", "INTEGER", meta, i++);
            assertColumn("BUFFER_LENGTH", "NULL", meta, i++);
            assertColumn("DECIMAL_DIGITS", "INTEGER", meta, i++);
            assertColumn("NUM_PREC_RADIX", "INTEGER", meta, i++);
            assertColumn("NULLABLE", "INTEGER", meta, i++);
            assertColumn("REMARKS", "VARCHAR", meta, i++);
            assertColumn("COLUMN_DEF", "VARCHAR", meta, i++);
            assertColumn("SQL_DATA_TYPE", "INTEGER", meta, i++);
            assertColumn("SQL_DATETIME_SUB", "INTEGER", meta, i++);
            assertColumn("CHAR_OCTET_LENGTH", "INTEGER", meta, i++);
            assertColumn("ORDINAL_POSITION", "INTEGER", meta, i++);
            assertColumn("IS_NULLABLE", "VARCHAR", meta, i++);
            assertColumn("SCOPE_CATALOG", "VARCHAR", meta, i++);
            assertColumn("SCOPE_SCHEMA", "VARCHAR", meta, i++);
            assertColumn("SCOPE_TABLE", "VARCHAR", meta, i++);
            assertColumn("SOURCE_DATA_TYPE", "SMALLINT", meta, i++);
            assertColumn("IS_AUTOINCREMENT", "VARCHAR", meta, i++);
            assertColumn("IS_GENERATEDCOLUMN", "VARCHAR", meta, i++);
            assertEquals(i - 1, meta.getColumnCount());

            assertTrue(results.next());
            i = 1;
            assertThat(results.getString(i++), startsWith("x-pack-elasticsearch_sql-clients_jdbc_"));
            assertEquals("", results.getString(i++));
            assertEquals("test.doc", results.getString(i++));
            assertEquals("name", results.getString(i++));
            assertEquals(Types.VARCHAR, results.getInt(i++));
            assertEquals("VARCHAR", results.getString(i++));
            assertEquals(1, results.getInt(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(10, results.getInt(i++)); // NOCOMMIT 10 seems wrong to hard code for stuff like strings
            // NOCOMMIT I think it'd be more correct to return DatabaseMetaData.columnNullable because all fields are nullable in es
            assertEquals(DatabaseMetaData.columnNullableUnknown, results.getInt(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(1, results.getInt(i++));
            assertEquals("", results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals(null, results.getString(i++));
            assertEquals("", results.getString(i++));
            assertEquals("", results.getString(i++));
            assertFalse(results.next());
        });

        // NOCOMMIT add some more columns and test that.
    }

    private static void assertColumn(String name, String type, ResultSetMetaData meta, int index) throws SQLException {
        assertEquals(name, meta.getColumnName(index));
        assertEquals(type, meta.getColumnTypeName(index));
    }
}
