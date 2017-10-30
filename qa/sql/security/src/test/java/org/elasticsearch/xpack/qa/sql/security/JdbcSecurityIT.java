/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.security;

import org.elasticsearch.xpack.qa.sql.jdbc.LocalH2;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.elasticsearch.xpack.qa.sql.jdbc.JdbcAssert.assertResultSets;
import static org.elasticsearch.xpack.qa.sql.jdbc.JdbcIntegrationTestCase.elasticsearchAddress;
import static org.hamcrest.Matchers.containsString;

public class JdbcSecurityIT extends SqlSecurityTestCase {
    static Properties adminProperties() {
        // tag::admin_properties
        Properties properties = new Properties();
        properties.put("user", "test_admin");
        properties.put("pass", "x-pack-test-password");
        // end::admin_properties
        return properties;
    }

    static Connection es(Properties properties) throws SQLException {
        return DriverManager.getConnection("jdbc:es://" + elasticsearchAddress(), properties);
    }

    private static class JdbcActions implements Actions {
        @Override
        public void queryWorksAsAdmin() throws Exception {
            try (Connection h2 = LocalH2.anonymousDb();
                    Connection es = es(adminProperties())) {
                h2.createStatement().executeUpdate("CREATE TABLE test (a BIGINT, b BIGINT, c BIGINT)");
                h2.createStatement().executeUpdate("INSERT INTO test (a, b, c) VALUES (1, 2, 3), (4, 5, 6)");

                ResultSet expected = h2.createStatement().executeQuery("SELECT * FROM test ORDER BY a");
                assertResultSets(expected, es.createStatement().executeQuery("SELECT * FROM test ORDER BY a"));
            }
        }

        @Override
        public void expectMatchesAdmin(String adminSql, String user, String userSql) throws Exception {
            try (Connection admin = es(adminProperties());
                    Connection other = es(userProperties(user))) {
                ResultSet expected = admin.createStatement().executeQuery(adminSql);
                assertResultSets(expected, other.createStatement().executeQuery(userSql));
            }
        }

        @Override
        public void expectScrollMatchesAdmin(String adminSql, String user, String userSql) throws Exception {
            try (Connection admin = es(adminProperties());
                    Connection other = es(userProperties(user))) {
                Statement adminStatement = admin.createStatement();
                adminStatement.setFetchSize(1);
                Statement otherStatement = other.createStatement();
                otherStatement.setFetchSize(1);
                assertResultSets(adminStatement.executeQuery(adminSql), otherStatement.executeQuery(userSql));
            }
        }

        @Override
        public void expectDescribe(Map<String, String> columns, String user) throws Exception {
            try (Connection h2 = LocalH2.anonymousDb();
                    Connection es = es(userProperties(user))) {
                // h2 doesn't have the same sort of DESCRIBE that we have so we emulate it
                h2.createStatement().executeUpdate("CREATE TABLE mock (column VARCHAR, type VARCHAR)");
                StringBuilder insert = new StringBuilder();
                insert.append("INSERT INTO mock (column, type) VALUES ");
                boolean first = true;
                for (Map.Entry<String, String> column : columns.entrySet()) {
                    if (first) {
                        first = false;
                    } else {
                        insert.append(", ");
                    }
                    insert.append("('").append(column.getKey()).append("', '").append(column.getValue()).append("')");
                }
                h2.createStatement().executeUpdate(insert.toString());

                ResultSet expected = h2.createStatement().executeQuery("SELECT * FROM mock");
                assertResultSets(expected, es.createStatement().executeQuery("DESCRIBE test"));
            }
        }

        @Override
        public void expectShowTables(List<String> tables, String user) throws Exception {
            try (Connection h2 = LocalH2.anonymousDb();
                    Connection es = es(userProperties(user))) {
                // h2 doesn't spit out the same columns we do so we emulate
                h2.createStatement().executeUpdate("CREATE TABLE mock (table VARCHAR)");
                StringBuilder insert = new StringBuilder();
                insert.append("INSERT INTO mock (table) VALUES ");
                boolean first = true;
                for (String table : tables) {
                    if (first) {
                        first = false;
                    } else {
                        insert.append(", ");
                    }
                    insert.append("('").append(table).append("')");
                }
                h2.createStatement().executeUpdate(insert.toString());

                ResultSet expected = h2.createStatement().executeQuery("SELECT * FROM mock ORDER BY table");
                assertResultSets(expected, es.createStatement().executeQuery("SHOW TABLES"));
            }
        }

        @Override
        public void expectForbidden(String user, String sql) throws Exception {
            SQLException e;
            try (Connection connection = es(userProperties(user))) {
                e = expectThrows(SQLException.class, () -> connection.createStatement().executeQuery(sql));
            }
            assertThat(e.getMessage(), containsString("is unauthorized for user [" + user + "]"));
        }

        @Override
        public void expectUnknownColumn(String user, String sql, String column) throws Exception {
            SQLException e;
            try (Connection connection = es(userProperties(user))) {
                e = expectThrows(SQLException.class, () -> connection.createStatement().executeQuery(sql));
            }
            assertThat(e.getMessage(), containsString("Unknown column [" + column + "]"));
        }

        private Properties userProperties(String user) {
            if (user == null) {
                return adminProperties();
            }
            Properties prop = new Properties();
            prop.put("user", user);
            prop.put("pass", "testpass");
            return prop;
        }
    }

    public JdbcSecurityIT() {
        super(new JdbcActions());
    }
}
