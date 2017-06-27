/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * Test the jdbc driver behavior and the connection to Elasticsearch.
 */
public class BasicsIT extends JdbcIntegrationTestCase {

    // NOCOMMIT these might should move into their own test or be deleted entirely
//    public void test01Ping() throws Exception {
//        assertThat(client.ping((int) TimeUnit.SECONDS.toMillis(5)), equalTo(true));
//    }
//
//    public void testInfoAction() throws Exception {
//        InfoResponse esInfo = client.serverInfo();
//        assertThat(esInfo, notNullValue());
//        assertThat(esInfo.cluster, is("elasticsearch"));
//        assertThat(esInfo.node, not(isEmptyOrNullString()));
//        assertThat(esInfo.versionHash, not(isEmptyOrNullString()));
//        assertThat(esInfo.versionString, startsWith("5."));
//        assertThat(esInfo.majorVersion, is(5));
//        //assertThat(esInfo.minorVersion(), is(0));
//    }
//
//    public void testInfoTable() throws Exception {
//        List<String> tables = client.metaInfoTables("emp*");
//        assertThat(tables.size(), greaterThanOrEqualTo(1));
//        assertThat(tables, hasItem("emp.emp"));
//    }
//
//    public void testInfoColumn() throws Exception {
//        List<MetaColumnInfo> info = client.metaInfoColumns("em*", null);
//        for (MetaColumnInfo i : info) {
//            // NOCOMMIT test these
//            logger.info(i);
//        }
//    }
    public void testConnectionProperties() throws SQLException {
        j.consume(c -> {
            assertFalse(c.isClosed());
            assertTrue(c.isReadOnly());
        });
    }

    /**
     * Tests that we throw report no transaction isolation and throw sensible errors if you ask for any.
     */
    public void testTransactionIsolation() throws Exception {
        j.consume(c -> {
            assertEquals(Connection.TRANSACTION_NONE, c.getTransactionIsolation());
            SQLException e = expectThrows(SQLException.class, () -> c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
            assertEquals("Transactions not supported", e.getMessage());
            assertEquals(Connection.TRANSACTION_NONE, c.getTransactionIsolation());
        });
    }

    public void testShowTablesEmpty() throws Exception {
        List<Map<String, Object>> results = j.queryForList("SHOW TABLES");
        assertEquals(emptyList(), results);
    }

    public void testShowTablesWithAnIndex() throws Exception {
        index("test", builder -> builder.field("name", "bob"));
        List<Map<String, Object>> results = j.queryForList("SHOW TABLES");
        List<Map<String, Object>> expected = new ArrayList<>();
        Map<String, Object> index = new HashMap<>();
        index.put("index", "test");
        index.put("type", "doc");
        expected.add(index);
        assertEquals(expected, results);
    }

    public void testShowTablesWithManyIndices() throws Exception {
        int indices = between(2, 20);
        for (int i = 0; i < indices; i++) {
            index("test" + i, builder -> builder.field("name", "bob"));
        }
        List<Map<String, Object>> results = j.queryForList("SHOW TABLES");
        results.sort(Comparator.comparing(map -> map.get("index").toString()));
        List<Map<String, Object>> expected = new ArrayList<>();
        for (int i = 0; i < indices; i++) {
            Map<String, Object> index = new HashMap<>();
            index.put("index", "test" + i);
            index.put("type", "doc");
            expected.add(index);
        }
        expected.sort(Comparator.comparing(map -> map.get("index").toString()));
        assertEquals(expected, results);

    }

    public void testBasicSelect() throws Exception {
        index("test", builder -> builder.field("name", "bob"));

        List<Map<String, Object>> results = j.queryForList("SELECT * from test.doc");
        assertEquals(singletonList(singletonMap("name", "bob")), results);
    }

    public void testSelectFromMissingTable() throws Exception {
        SQLException e = expectThrows(SQLException.class, () -> j.queryForList("SELECT * from test.doc"));
        assertEquals("line 1:15: Cannot resolve index test", e.getMessage());
    }

    public void testSelectFromMissingType() throws Exception {
        index("test", builder -> builder.field("name", "bob"));

        SQLException e = expectThrows(SQLException.class, () -> j.queryForList("SELECT * from test.notdoc"));
        assertEquals("line 1:15: Cannot resolve type notdoc in index test", e.getMessage());
    }
}