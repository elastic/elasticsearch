/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.jdbc;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.qa.sql.embed.EmbeddedJdbcServer;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConfiguration;
import org.elasticsearch.xpack.sql.jdbc.jdbcx.JdbcDataSource;
import org.joda.time.DateTimeZone;
import org.junit.ClassRule;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

import static java.util.Collections.singletonMap;

public abstract class JdbcIntegrationTestCase extends ESRestTestCase {
    /**
     * Should the HTTP server that serves SQL be embedded in the test
     * process (true) or should the JDBC driver connect to Elasticsearch
     * running at {@code tests.rest.cluster}. Note that to use embedded
     * HTTP you have to have Elasticsearch's transport protocol open on
     * port 9300 but the Elasticsearch running there does not need to have
     * the SQL plugin installed. Note also that embedded HTTP is faster
     * but is not canonical because it runs against a different HTTP server
     * then JDBC will use in production. Gradle always uses non-embedded.
     */
    protected static final boolean EMBED_SQL = Booleans.parseBoolean(System.getProperty("tests.embed.sql", "false"));

    @ClassRule
    public static final EmbeddedJdbcServer EMBEDDED_SERVER = EMBED_SQL ? new EmbeddedJdbcServer() : null;

    /**
     * Read an address for Elasticsearch suitable for the JDBC driver from the system properties.
     */
    public static String elasticsearchAddress() {
        String cluster = System.getProperty("tests.rest.cluster");
        // JDBC only supports a single node at a time so we just give it one.
        return cluster.split(",")[0];
        /* This doesn't include "jdbc:es://" because we want the example in
         * esJdbc to be obvious. */
    }

    public Connection esJdbc() throws SQLException {
        if (EMBED_SQL) {
            return EMBEDDED_SERVER.connection(connectionProperties());
        }
        return randomBoolean() ? useDriverManager() : useDataSource();
    }

    protected Connection useDriverManager() throws SQLException {
        // tag::connect-dm
        String address = "jdbc:es://" + elasticsearchAddress();   // <1>
        Properties connectionProperties = connectionProperties(); // <2>
        return DriverManager.getConnection(address, connectionProperties);
        // end::connect-dm
    }

    protected Connection useDataSource() throws SQLException {
        // tag::connect-ds
        JdbcDataSource dataSource = new JdbcDataSource();
        String address = "jdbc:es://" + elasticsearchAddress();   // <1>
        dataSource.setUrl(address);
        Properties connectionProperties = connectionProperties(); // <2>
        dataSource.setProperties(connectionProperties);
        return dataSource.getConnection();
        // end::connect-ds
    }

    public static void index(String index, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        body.accept(builder);
        builder.endObject();
        HttpEntity doc = new StringEntity(builder.string(), ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/" + index + "/doc/1", singletonMap("refresh", "true"), doc);
    }

    protected String clusterName() {
        try {
            String response = EntityUtils.toString(client().performRequest("GET", "/").getEntity());
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
        connectionProperties.put(JdbcConfiguration.TIME_ZONE, randomKnownTimeZone());
        return connectionProperties;
    }

    public static String randomKnownTimeZone() {
        // We use system default timezone for the connection that is selected randomly by TestRuleSetupAndRestoreClassEnv
        // from all available JDK timezones. While Joda and JDK are generally in sync, some timezones might not be known
        // to the current version of Joda and in this case the test might fail. To avoid that, we specify a timezone
        // known for both Joda and JDK
        Set<String> timeZones = new HashSet<>(DateTimeZone.getAvailableIDs());
        timeZones.retainAll(Arrays.asList(TimeZone.getAvailableIDs()));
        List<String> ids = new ArrayList<>(timeZones);
        Collections.sort(ids);
        return randomFrom(ids);
    }
}