/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class JdbcQueryBuilderTests extends ESTestCase {

    private final JdbcQueryBuilder builder = new JdbcQueryBuilder(GenericDialect.INSTANCE);

    private String sqlOf(List<String> cols, String catalog, String schema, String table, int rowLimit) {
        return builder.buildScan(cols, catalog, schema, table, rowLimit, null).sql();
    }

    public void testBuildScanTableOnly() {
        String table = randomAlphaOfLength(8);
        assertEquals("SELECT \"col_a\", \"col_b\" FROM \"" + table + "\"", sqlOf(List.of("col_a", "col_b"), null, null, table, 0));
    }

    public void testBuildScanWithSchema() {
        String schema = randomAlphaOfLength(6);
        String table = randomAlphaOfLength(8);
        assertEquals("SELECT \"id\" FROM \"" + schema + "\".\"" + table + "\"", sqlOf(List.of("id"), null, schema, table, 0));
    }

    public void testBuildScanWithCatalogAndSchema() {
        String catalog = randomAlphaOfLength(5);
        String schema = randomAlphaOfLength(6);
        String table = randomAlphaOfLength(8);
        assertEquals(
            "SELECT \"x\" FROM \"" + catalog + "\".\"" + schema + "\".\"" + table + "\"",
            sqlOf(List.of("x"), catalog, schema, table, 0)
        );
    }

    public void testBuildScanWithLimit() {
        int limit = randomIntBetween(1, 1000);
        String table = randomAlphaOfLength(8);
        assertEquals("SELECT \"a\" FROM \"" + table + "\" LIMIT " + limit, sqlOf(List.of("a"), null, null, table, limit));
    }

    public void testBuildScanWithoutLimit() {
        String table = randomAlphaOfLength(8);
        assertFalse(sqlOf(List.of("a"), null, null, table, 0).contains("LIMIT"));
    }

    public void testBuildScanIdentifierQuoting() {
        String col = randomAlphaOfLength(10);
        String table = randomAlphaOfLength(10);
        String sql = sqlOf(List.of(col), null, null, table, 0);
        assertTrue(sql.contains("\"" + col + "\""));
        assertTrue(sql.contains("\"" + table + "\""));
    }

    public void testBuildScanEmptyProjectionEmitsSelectOne() {
        String table = randomAlphaOfLength(8);
        assertEquals("SELECT 1 FROM \"" + table + "\"", sqlOf(List.of(), null, null, table, 0));
    }

    public void testBuildScanEmptyProjectionWithLimit() {
        int limit = randomIntBetween(1, 1000);
        String table = randomAlphaOfLength(8);
        assertEquals("SELECT 1 FROM \"" + table + "\" LIMIT " + limit, sqlOf(List.of(), null, null, table, limit));
    }

    public void testBuildScanRejectsNullProjection() {
        expectThrows(IllegalArgumentException.class, () -> builder.buildScan(null, null, null, "t", 0, null));
    }

    public void testBuildScanRejectsNullTable() {
        expectThrows(IllegalArgumentException.class, () -> builder.buildScan(List.of("a"), null, null, null, 0, null));
    }

    public void testBuildScanRejectsEmptyTable() {
        expectThrows(IllegalArgumentException.class, () -> builder.buildScan(List.of("a"), null, null, "", 0, null));
    }

    public void testBuildScanWithPushedFilterRendersWhereAndCollectsParams() {
        SqlPredicate predicate = new SqlPredicate.Comparison(
            "age",
            CompOp.GT,
            new SqlParam(18, org.elasticsearch.xpack.esql.core.type.DataType.INTEGER)
        );
        JdbcQueryBuilder.BuiltScan built = builder.buildScan(List.of("name"), null, null, "users", 0, new JdbcPushedQuery(predicate));
        assertEquals("SELECT \"name\" FROM \"users\" WHERE \"age\" > ?", built.sql());
        assertEquals(1, built.params().size());
        assertEquals(18, built.params().get(0).value());
    }

    public void testBuildScanPushedFilterAppearsBeforeLimit() {
        SqlPredicate predicate = new SqlPredicate.IsNotNull("c");
        JdbcQueryBuilder.BuiltScan built = builder.buildScan(List.of("c"), null, null, "t", 5, new JdbcPushedQuery(predicate));
        assertEquals("SELECT \"c\" FROM \"t\" WHERE \"c\" IS NOT NULL LIMIT 5", built.sql());
    }
}
