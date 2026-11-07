/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.test.ESTestCase;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

public class JdbcDatabaseMetaDataTests extends ESTestCase {

    private JdbcDatabaseMetaData md = null;

    {
        try {
            md = new JdbcDatabaseMetaData(
                new JdbcConnection(JdbcConfiguration.create("jdbc:es://localhost:9200/", new Properties(), 10), false)
            );
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

    public void testGetNumericFunctions() throws Exception {
        Set<String> expected = Set.of(
            "ABS",
            "ACOS",
            "ASIN",
            "ATAN",
            "ATAN2",
            "CEILING",
            "COS",
            "DEGREES",
            "EXP",
            "FLOOR",
            "LOG",
            "LOG10",
            "MOD",
            "PI",
            "POWER",
            "RADIANS",
            "RAND",
            "ROUND",
            "SIGN",
            "SIN",
            "SQRT",
            "TAN",
            "TRUNCATE"
        );
        String[] numericFunctions = md.getNumericFunctions().split(",");
        assertEquals(expected, Set.of(numericFunctions));
        // check the list does not have duplicates
        assertEquals(expected.size(), numericFunctions.length);
    }

    public void testGetStringFunctions() throws Exception {
        Set<String> expected = Set.of(
            "ASCII",
            "BIT_LENGTH",
            "CHAR",
            "CHAR_LENGTH",
            "CHARACTER_LENGTH",
            "CONCAT",
            "INSERT",
            "LCASE",
            "LEFT",
            "LENGTH",
            "LOCATE",
            "LTRIM",
            "OCTET_LENGTH",
            "POSITION",
            "REPEAT",
            "REPLACE",
            "RIGHT",
            "RTRIM",
            "SPACE",
            "SUBSTRING",
            "UCASE"
        );
        String[] stringFunctions = md.getStringFunctions().split(",");
        assertEquals(expected, Set.of(stringFunctions));
        // check the list does not have duplicates
        assertEquals(expected.size(), stringFunctions.length);
    }

    public void testGetSystemFunctions() throws Exception {
        Set<String> expected = Set.of("DATABASE", "IFNULL", "USER");
        String[] systemFunctions = md.getSystemFunctions().split(",");
        assertEquals(expected, Set.of(systemFunctions));
        // check the list does not have duplicates
        assertEquals(expected.size(), systemFunctions.length);
    }

    public void testGetTimeDateFunctions() throws Exception {
        Set<String> expected = Set.of(
            "DAYNAME",
            "DAYOFMONTH",
            "DAYOFWEEK",
            "DAYOFYEAR",
            "EXTRACT",
            "HOUR",
            "MINUTE",
            "MONTH",
            "MONTHNAME",
            "QUARTER",
            "SECOND",
            "WEEK",
            "YEAR"
        );
        String[] timeDateFunctions = md.getTimeDateFunctions().split(",");
        assertEquals(expected, Set.of(timeDateFunctions));
        // check the list does not have duplicates
        assertEquals(expected.size(), timeDateFunctions.length);
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
