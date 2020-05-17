/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.security;

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.io.PathUtils;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.lucene.util.LuceneTestCase.expectThrows;
import static org.apache.lucene.util.LuceneTestCase.getTestClass;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase.elasticsearchAddress;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase.randomKnownTimeZone;
import static org.elasticsearch.xpack.sql.qa.security.JdbcConnectionIT.SSL_ENABLED;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class JdbcSecurityIT {

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
            keyStore = PathUtils.get(getTestClass().getResource("/test-node.jks").toURI());
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

    static void expectActionThrowsUnknownColumn(String user, CheckedConsumer<Connection, SQLException> action, String column)
        throws Exception {
        SQLException e;
        try (Connection connection = es(userProperties(user))) {
            e = expectThrows(SQLException.class, () -> action.accept(connection));
        }
        assertThat(e.getMessage(), containsString("Unknown column [" + column + "]"));
    }
}
