/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.sql.jdbc.EsDataSource;
import org.junit.After;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public abstract class JdbcIntegrationTestCase extends ESRestTestCase {

    public static final String JDBC_ES_URL_PREFIX = "jdbc:es://";

    @After
    public void checkSearchContent() throws IOException {
        // Some context might linger due to fire and forget nature of PIT cleanup
        assertNoSearchContexts();
    }

    /**
     * Read an address for Elasticsearch suitable for the JDBC driver from the system properties.
     */
    public String elasticsearchAddress() {
        String cluster = getTestRestCluster();
        // JDBC only supports a single node at a time so we just give it one.
        return cluster.split(",")[0];
        /* This doesn't include "jdbc:es://" because we want the example in
         * esJdbc to be obvious and because we want to use getProtocol to add
         * https if we are running against https. */
    }

    public Connection esJdbc() throws SQLException {
        return esJdbc(connectionProperties());
    }

    public Connection esJdbc(Properties props) throws SQLException {
        return createConnection(props);
    }

    protected Connection createConnection(Properties connectionProperties) throws SQLException {
        String elasticsearchAddress = getProtocol() + "://" + elasticsearchAddress();
        String address = JDBC_ES_URL_PREFIX + elasticsearchAddress;
        Connection connection;
        if (randomBoolean()) {
            connection = DriverManager.getConnection(address, connectionProperties);
        } else {
            EsDataSource dataSource = new EsDataSource();
            dataSource.setUrl(address);
            dataSource.setProperties(connectionProperties);
            connection = dataSource.getConnection();
        }

        assertNotNull("The timezone should be specified", connectionProperties.getProperty("timezone"));
        return connection;
    }

    //
    // methods below are used inside the documentation only
    //
    protected Connection useDriverManager() throws SQLException {
        String elasticsearchAddress = getProtocol() + "://" + elasticsearchAddress();
        // tag::connect-dm
        String address = "jdbc:es://" + elasticsearchAddress;     // <1>
        Properties connectionProperties = connectionProperties(); // <2>
        Connection connection =
            DriverManager.getConnection(address, connectionProperties);
        // end::connect-dm
        assertNotNull("The timezone should be specified", connectionProperties.getProperty("timezone"));
        return connection;
    }

    protected Connection useDataSource() throws SQLException {
        String elasticsearchAddress = getProtocol() + "://" + elasticsearchAddress();
        // tag::connect-ds
        EsDataSource dataSource = new EsDataSource();
        String address = "jdbc:es://" + elasticsearchAddress;     // <1>
        dataSource.setUrl(address);
        Properties connectionProperties = connectionProperties(); // <2>
        dataSource.setProperties(connectionProperties);
        Connection connection = dataSource.getConnection();
        // end::connect-ds
        assertNotNull("The timezone should be specified", connectionProperties.getProperty("timezone"));
        return connection;
    }

    public static void index(String index, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        index(index, "1", body);
    }

    public static void index(String index, String documentId, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_doc/" + documentId);
        request.addParameter("refresh", "true");
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        body.accept(builder);
        builder.endObject();
        request.setJsonEntity(Strings.toString(builder));
        client().performRequest(request);
    }

    public static void delete(String index, String documentId) throws IOException {
        Request request = new Request("DELETE", "/" + index + "/_doc/" + documentId);
        request.addParameter("refresh", "true");
        client().performRequest(request);
    }

    /**
     * The properties used to build the connection.
     */
    protected Properties connectionProperties() {
        Properties connectionProperties = new Properties();
        connectionProperties.put(JdbcTestUtils.JDBC_TIMEZONE, randomZone().getId());
        // in the tests, don't be lenient towards multi values
        connectionProperties.put("field.multi.value.leniency", "false");
        return connectionProperties;
    }

    private static Map<String, Object> searchStats() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_stats/search"));
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }

    @SuppressWarnings("unchecked")
    private static int getOpenContexts(Map<String, Object> stats, String index) {
        stats = (Map<String, Object>) stats.get("indices");
        stats = (Map<String, Object>) stats.get(index);
        stats = (Map<String, Object>) stats.get("total");
        stats = (Map<String, Object>) stats.get("search");
        return (Integer) stats.get("open_contexts");
    }

    static void assertNoSearchContexts() throws IOException {
        Map<String, Object> stats = searchStats();
        @SuppressWarnings("unchecked")
        Map<String, Object> indicesStats = (Map<String, Object>) stats.get("indices");
        for (String index : indicesStats.keySet()) {
            if (index.startsWith(".") == false) { // We are not interested in internal indices
                assertEquals(index + " should have no search contexts", 0, getOpenContexts(stats, index));
            }
        }
    }

    @Override
    protected String getTestRestCluster() {
        return getCluster().getHttpAddresses();
    }

    public abstract ElasticsearchCluster getCluster();

    protected static ElasticsearchCluster singleNodeCluster() {
        return ElasticsearchCluster.local()
            .module("x-pack-sql")
            .module("constant-keyword")
            .module("wildcard")
            .module("unsigned-long")
            .module("mapper-version")
            .module("rest-root")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .build();
    }

    protected static ElasticsearchCluster multiNodeCluster() {
        return ElasticsearchCluster.local()
            .nodes(2)
            .module("x-pack-sql")
            .module("constant-keyword")
            .module("wildcard")
            .module("rest-root")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .build();
    }

    protected static ElasticsearchCluster withSecurityCluster(boolean withSsl) {
        return ElasticsearchCluster.local()
            .module("x-pack-sql")
            .module("constant-keyword")
            .module("wildcard")
            .module("rest-root")
            .setting("xpack.security.audit.enabled", "true")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.http.ssl.enabled", String.valueOf(withSsl))
            .setting("xpack.security.transport.ssl.enabled", String.valueOf(withSsl))
            .setting("xpack.security.transport.ssl.keystore.path", "test-node.jks")
            .setting("xpack.security.http.ssl.keystore.path", "test-node.jks")
            .keystore("xpack.security.transport.ssl.keystore.secure_password", "keypass")
            .keystore("xpack.security.http.ssl.keystore.secure_password", "keypass")
            .configFile("test-node.jks", Resource.fromClasspath("test-node.jks"))
            .configFile("test-client.jks", Resource.fromClasspath("test-client.jks"))
            .user("test_admin", "x-pack-test-password")
            .build();
    }

    protected static Settings securitySettings(boolean isSslEnabled) {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        Settings.Builder builder = Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token);
        if (isSslEnabled) {
            Path keyStore;
            try {
                keyStore = PathUtils.get(getTestClass().getResource("/test-node.jks").toURI());
            } catch (URISyntaxException e) {
                throw new RuntimeException("exception while reading the store", e);
            }
            if (Files.exists(keyStore) == false) {
                throw new IllegalStateException("Keystore file [" + keyStore + "] does not exist.");
            }
            builder.put(ESRestTestCase.TRUSTSTORE_PATH, keyStore).put(ESRestTestCase.TRUSTSTORE_PASSWORD, "keypass");
        }
        return builder.build();
    }
}
