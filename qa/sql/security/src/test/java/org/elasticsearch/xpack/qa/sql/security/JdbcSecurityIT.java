/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.security;

import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.xpack.qa.sql.jdbc.LocalH2;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.elasticsearch.xpack.qa.sql.jdbc.JdbcAssert.assertResultSets;
import static org.elasticsearch.xpack.qa.sql.jdbc.JdbcIntegrationTestCase.elasticsearchAddress;
import static org.elasticsearch.xpack.qa.sql.jdbc.JdbcIntegrationTestCase.randomKnownTimeZone;
import static org.elasticsearch.xpack.qa.sql.security.RestSqlIT.SSL_ENABLED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

public class JdbcSecurityIT extends SqlSecurityTestCase {
    static Properties adminProperties() {
        // tag::admin_properties
        Properties properties = new Properties();
        properties.put("user", "test_admin");
        properties.put("password", "x-pack-test-password");
        // end::admin_properties
        addSslPropertiesIfNeeded(properties);
        return properties;
    }

    static Connection es(Properties properties) throws SQLException {
        Properties props = new Properties();
        props.put("timezone", randomKnownTimeZone());
        props.putAll(properties);
        String scheme = SSL_ENABLED ? "https" : "http";
        return DriverManager.getConnection("jdbc:es://" + scheme + "://" + elasticsearchAddress(), props);
    }

    static Properties userProperties(String user) {
        if (user == null) {
            return adminProperties();
        }
        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", "testpass");
        addSslPropertiesIfNeeded(prop);
        return prop;
    }

    private static void addSslPropertiesIfNeeded(Properties properties) {
        if (false == SSL_ENABLED) {
            return;
        }
        Path keyStore;
        try {
            keyStore = PathUtils.get(RestSqlIT.class.getResource("/test-node.jks").toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException("exception while reading the store", e);
        }
        if (!Files.exists(keyStore)) {
            throw new IllegalStateException("Keystore file [" + keyStore + "] does not exist.");
        }
        String keyStoreStr = keyStore.toAbsolutePath().toString();

        properties.put("ssl", "true");
        properties.put("ssl.keystore.location", keyStoreStr);
        properties.put("ssl.keystore.pass", "keypass");
        properties.put("ssl.truststore.location", keyStoreStr);
        properties.put("ssl.truststore.pass", "keypass");
    }

    static void expectActionMatchesAdmin(CheckedFunction<Connection, ResultSet, SQLException> adminAction,
            String user, CheckedFunction<Connection, ResultSet, SQLException> userAction) throws Exception {
        try (Connection adminConnection = es(adminProperties());
                Connection userConnection = es(userProperties(user))) {
            assertResultSets(adminAction.apply(adminConnection), userAction.apply(userConnection));
        }
    }

    static void expectForbidden(String user, CheckedConsumer<Connection, SQLException> action) throws Exception {
        expectError(user, action, "is unauthorized for user [" + user + "]");
    }

    static void expectUnknownIndex(String user, CheckedConsumer<Connection, SQLException> action) throws Exception {
        expectError(user, action, "Unknown index");
    }

    static void expectError(String user, CheckedConsumer<Connection, SQLException> action, String errorMessage) throws Exception {
        SQLException e;
        try (Connection connection = es(userProperties(user))) {
            e = expectThrows(SQLException.class, () -> action.accept(connection));
        }
        assertThat(e.getMessage(), containsString(errorMessage));
    }

    static void expectActionThrowsUnknownColumn(String user,
            CheckedConsumer<Connection, SQLException> action, String column) throws Exception {
        SQLException e;
        try (Connection connection = es(userProperties(user))) {
            e = expectThrows(SQLException.class, () -> action.accept(connection));
        }
        assertThat(e.getMessage(), containsString("Unknown column [" + column + "]"));
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
            expectActionMatchesAdmin(
                con -> con.createStatement().executeQuery(adminSql),
                user,
                con -> con.createStatement().executeQuery(userSql));
        }

        @Override
        public void expectScrollMatchesAdmin(String adminSql, String user, String userSql) throws Exception {
            expectActionMatchesAdmin(
                con -> {
                    Statement st = con.createStatement();
                    st.setFetchSize(1);
                    return st.executeQuery(adminSql);
                },
                user,
                con -> {
                    Statement st = con.createStatement();
                    st.setFetchSize(1);
                    return st.executeQuery(userSql);
                });
        }

        @Override
        public void expectDescribe(Map<String, String> columns, String user) throws Exception {
            try (Connection h2 = LocalH2.anonymousDb();
                    Connection es = es(userProperties(user))) {
                // h2 doesn't have the same sort of DESCRIBE that we have so we emulate it
                h2.createStatement().executeUpdate("CREATE TABLE mock (column VARCHAR, type VARCHAR)");
                if (columns.size() > 0) {
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
                }

                ResultSet expected = h2.createStatement().executeQuery("SELECT * FROM mock");
                assertResultSets(expected, es.createStatement().executeQuery("DESCRIBE test"));
            }
        }

        @Override
        public void expectShowTables(List<String> tables, String user) throws Exception {
            try (Connection es = es(userProperties(user))) {
                ResultSet actual = es.createStatement().executeQuery("SHOW TABLES");

                // depending on whether security is enabled and a test is run in isolation or suite
                // .security or .security6 index can appear
                // to filter these out, the result set is flatten to a list
                List<String> actualList = new ArrayList<>();

                while (actual.next()) {
                    String name = actual.getString("name");
                    if (!name.startsWith(".security")) {
                        actualList.add(name);
                    }
                }

                assertEquals(tables, actualList);
            }
        }

        @Override
        public void expectForbidden(String user, String sql) throws Exception {
            JdbcSecurityIT.expectForbidden(user, con -> con.createStatement().executeQuery(sql));
        }

        @Override
        public void expectUnknownIndex(String user, String sql) throws Exception {
            JdbcSecurityIT.expectUnknownIndex(user, con -> con.createStatement().executeQuery(sql));
        }

        @Override
        public void expectUnknownColumn(String user, String sql, String column) throws Exception {
            expectActionThrowsUnknownColumn(
                user,
                con -> con.createStatement().executeQuery(sql),
                column);
        }
    }

    public JdbcSecurityIT() {
        super(new JdbcActions());
    }

    // Metadata methods only available to JDBC
    public void testMetaDataGetTablesWithFullAccess() throws Exception {
        createUser("full_access", "read_all");

        expectActionMatchesAdmin(
            con -> con.getMetaData().getTables("%", "%", "%t", null),
            "full_access",
            con -> con.getMetaData().getTables("%", "%", "%", null));
    }

    public void testMetaDataGetTablesWithNoAccess() throws Exception {
        createUser("no_access", "read_nothing");

        expectForbidden("no_access", con -> con.getMetaData().getTables("%", "%", "%", null));
    }

    public void testMetaDataGetTablesWithLimitedAccess() throws Exception {
        createUser("read_bort", "read_bort");

        expectActionMatchesAdmin(
            con -> con.getMetaData().getTables("%", "%", "bort", null),
            "read_bort",
            con -> con.getMetaData().getTables("%", "%", "%", null));
    }

    public void testMetaDataGetTablesWithInAccessibleIndex() throws Exception {
        createUser("read_bort", "read_bort");

        expectActionMatchesAdmin(
            con -> con.getMetaData().getTables("%", "%", "not_created", null),
            "read_bort",
            con -> con.getMetaData().getTables("%", "%", "test", null));
    }

    public void testMetaDataGetColumnsWorksAsFullAccess() throws Exception {
        createUser("full_access", "read_all");

        expectActionMatchesAdmin(
            con -> con.getMetaData().getColumns("%", "%", "%t", "%"),
            "full_access",
            con -> con.getMetaData().getColumns("%", "%", "%", "%"));
        new AuditLogAsserter()
            .expect(true, GetIndexAction.NAME, "test_admin", contains("bort", "test"))
            .expect(true, GetIndexAction.NAME, "full_access", contains("bort", "test"))
            .assertLogs();
    }

    public void testMetaDataGetColumnsWithNoAccess() throws Exception {
        createUser("no_access", "read_nothing");

        expectForbidden("no_access", con -> con.getMetaData().getColumns("%", "%", "%", "%"));
        new AuditLogAsserter()
            // TODO figure out why this generates *no* logs
            // .expect(false, GetIndexAction.NAME, "no_access", contains("bort", "test"))
            .assertLogs();
    }

    public void testMetaDataGetColumnsWithWrongAccess() throws Exception {
        createUser("wrong_access", "read_something_else");

        expectActionMatchesAdmin(
            con -> con.getMetaData().getColumns("%", "%", "not_created", "%"),
            "wrong_access",
            con -> con.getMetaData().getColumns("%", "%", "test", "%"));
        new AuditLogAsserter()
            .expect(true, GetIndexAction.NAME, "test_admin", contains("*", "-*"))
            .expect(true, GetIndexAction.NAME, "wrong_access", contains("*", "-*"))
            .assertLogs();
    }

    public void testMetaDataGetColumnsSingleFieldGranted() throws Exception {
        createUser("only_a", "read_test_a");

        expectActionMatchesAdmin(
            con -> con.getMetaData().getColumns("%", "%", "test", "a"),
            "only_a",
            con -> con.getMetaData().getColumns("%", "%", "test", "%"));
        new AuditLogAsserter()
            .expect(true, GetIndexAction.NAME, "test_admin", contains("test"))
            .expect(true, GetIndexAction.NAME, "only_a", contains("test"))
            .assertLogs();
    }

    public void testMetaDataGetColumnsSingleFieldExcepted() throws Exception {
        createUser("not_c", "read_test_a_and_b");

        /* Since there is no easy way to get a result from the admin side with
         * both 'a' and 'b' we'll have to roll our own assertion here, but we
         * are intentionally much less restrictive then the tests elsewhere. */
        try (Connection con = es(userProperties("not_c"))) {
            ResultSet result = con.getMetaData().getColumns("%", "%", "test", "%");
            assertTrue(result.next());
            String columnName = result.getString(4);
            assertEquals("a", columnName);
            assertTrue(result.next());
            columnName = result.getString(4);
            assertEquals("b", columnName);
            assertFalse(result.next());
        }
        new AuditLogAsserter()
            .expect(true, GetIndexAction.NAME, "not_c", contains("test"))
            .assertLogs();
    }

    public void testMetaDataGetColumnsDocumentExcluded() throws Exception {
        createUser("no_3s", "read_test_without_c_3");

        expectActionMatchesAdmin(
            con -> con.getMetaData().getColumns("%", "%", "test", "%"),
            "no_3s",
            con -> con.getMetaData().getColumns("%", "%", "test", "%"));
        new AuditLogAsserter()
            .expect(true, GetIndexAction.NAME, "test_admin", contains("test"))
            .expect(true, GetIndexAction.NAME, "no_3s", contains("test"))
            .assertLogs();
    }
}
