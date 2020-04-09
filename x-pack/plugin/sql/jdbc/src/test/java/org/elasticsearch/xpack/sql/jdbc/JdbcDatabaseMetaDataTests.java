/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.test.ESTestCase;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcDatabaseMetaDataTests extends ESTestCase {

    private JdbcDatabaseMetaData md = null;

    {
        try {
            md = new JdbcDatabaseMetaData(
                    new JdbcConnection(JdbcConfiguration.create("jdbc:es://localhost:9200/", new Properties(), 10), false));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void testSeparators() throws Exception {
        assertEquals(":", md.getCatalogSeparator());
        assertEquals("\"", md.getIdentifierQuoteString());
        assertEquals("\\", md.getSearchStringEscape());
    }

    public void testGetProcedures() throws Exception {
        testEmptySet(() -> md.getProcedures(null, null, null));
    }

    public void testGetProcedureColumns() throws Exception {
        testEmptySet(() -> md.getProcedureColumns(null, null, null, null));
    }

    public void testGetColumnPrivileges() throws Exception {
        testEmptySet(() -> md.getColumnPrivileges(null, null, null, null));
    }

    public void testGetTablePrivileges() throws Exception {
        testEmptySet(() -> md.getTablePrivileges(null, null, null));
    }

    public void testGetBestRowIdentifier() throws Exception {
        testEmptySet(() -> md.getBestRowIdentifier(null, null, null, 0, false));
    }

    public void testGetVersionColumns() throws Exception {
        testEmptySet(() -> md.getVersionColumns(null, null, null));
    }

    public void testGetPrimaryKeys() throws Exception {
        testEmptySet(() -> md.getPrimaryKeys(null, null, null));
    }

    public void testGetImportedKeys() throws Exception {
        testEmptySet(() -> md.getImportedKeys(null, null, null));
    }

    public void testGetExportedKeys() throws Exception {
        testEmptySet(() -> md.getExportedKeys(null, null, null));
    }

    public void testGetCrossReference() throws Exception {
        testEmptySet(() -> md.getCrossReference(null, null, null, null, null, null));
    }

    public void testGetIndexInfo() throws Exception {
        testEmptySet(() -> md.getIndexInfo(null, null, null, false, false));
    }

    public void testGetUDTs() throws Exception {
        testEmptySet(() -> md.getUDTs(null, null, null, null));
    }

    public void testGetSuperTypes() throws Exception {
        testEmptySet(() -> md.getSuperTypes(null, null, null));
    }

    public void testGetSuperTables() throws Exception {
        testEmptySet(() -> md.getSuperTables(null, null, null));
    }

    public void testGetAttributes() throws Exception {
        testEmptySet(() -> md.getAttributes(null, null, null, null));
    }

    public void testGetFunctions() throws Exception {
        testEmptySet(() -> md.getFunctions(null, null, null));
    }

    public void testGetFunctionColumns() throws Exception {
        testEmptySet(() -> md.getFunctionColumns(null, null, null, null));
    }

    public void testGetPseudoColumns() throws Exception {
        testEmptySet(() -> md.getPseudoColumns(null, null, null, null));
    }

    public void testGetSchemas() throws Exception {
        testEmptySet(() -> md.getSchemas());
        testEmptySet(() -> md.getSchemas(null, null));
    }

    private static void testEmptySet(CheckedSupplier<ResultSet, SQLException> supplier) throws SQLException {
        try (ResultSet result = supplier.get()) {
            assertNotNull(result);
            assertFalse(result.next());
        }
    }

    public void testGetClientInfoProperties() throws Exception {
        try (ResultSet result = md.getClientInfoProperties()) {
            assertNotNull(result);
            assertTrue(result.next());
            assertNotNull(result.getString(1));
            assertEquals(-1, result.getInt(2));
            assertEquals("", result.getString(3));
            assertEquals("", result.getString(4));
        }
    }
}