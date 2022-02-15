/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.sql.jdbc.EsDataSource;
import org.elasticsearch.xpack.sql.qa.rest.RemoteClusterAwareSqlRestTestCase;
import org.junit.After;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.xpack.ql.TestUtils.assertNoSearchContexts;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.JDBC_TIMEZONE;

public abstract class JdbcIntegrationTestCase extends RemoteClusterAwareSqlRestTestCase {

    @After
    public void checkSearchContent() throws Exception {
        // Some context might linger due to fire and forget nature of PIT cleanup
        assertNoSearchContexts(provisioningClient());
    }

    /**
     * Read an address for Elasticsearch suitable for the JDBC driver from the system properties.
     */
    public static String elasticsearchAddress() {
        String cluster = System.getProperty("tests.rest.cluster");
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
        String address = "jdbc:es://" + elasticsearchAddress;
        Connection connection = null;
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
        provisioningClient().performRequest(request);
    }

    public static void delete(String index, String documentId) throws IOException {
        Request request = new Request("DELETE", "/" + index + "/_doc/" + documentId);
        request.addParameter("refresh", "true");
        provisioningClient().performRequest(request);
    }

    protected String clusterName() {
        try {
            String response = EntityUtils.toString(client().performRequest(new Request("GET", "/")).getEntity());
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false).get("cluster_name").toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The properties used to build the connection.
     */
    protected Properties connectionProperties() {
        Properties connectionProperties = new Properties();
        connectionProperties.put(JDBC_TIMEZONE, randomZone().getId());
        // in the tests, don't be lenient towards multi values
        connectionProperties.put("field.multi.value.leniency", "false");
        if (hasText(AUTH_USER) && hasText(AUTH_PASS)) {
            connectionProperties.put("user", AUTH_USER);
            connectionProperties.put("password", AUTH_PASS);
        }
        return connectionProperties;
    }

    protected static void createIndex(String index) throws IOException {
        Request request = new Request("PUT", "/" + index);
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("settings");
        {
            createIndex.field("number_of_shards", 1);
            createIndex.field("number_of_replicas", 1);
        }
        createIndex.endObject();
        createIndex.startObject("mappings");
        {
            createIndex.startObject("properties");
            createIndex.endObject();
        }
        createIndex.endObject().endObject();
        request.setJsonEntity(Strings.toString(createIndex));
        provisioningClient().performRequest(request);
    }

    protected static void updateMapping(String index, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_mapping");
        XContentBuilder updateMapping = JsonXContent.contentBuilder().startObject();
        updateMapping.startObject("properties");
        {
            body.accept(updateMapping);
        }
        updateMapping.endObject().endObject();

        request.setJsonEntity(Strings.toString(updateMapping));
        provisioningClient().performRequest(request);
    }
}
