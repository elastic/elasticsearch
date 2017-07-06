/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.rules.ExternalResource;

import java.net.InetAddress;
import java.security.AccessControlException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

/**
 * Embedded JDBC server that uses the transport client to power
 * the jdbc endpoints in the same JVM as the tests.
 */
public class EmbeddedJdbcServer extends ExternalResource implements CheckedSupplier<Connection, SQLException> {

    private Client client;
    private JdbcHttpServer server;
    private String jdbcUrl;
    private final Properties properties;

    public EmbeddedJdbcServer() {
        this(false);
    }

    public EmbeddedJdbcServer(boolean debug) {
        properties = new Properties();
        if (debug) {
            properties.setProperty("debug", "true");
        }
    }

    @Override
    @SuppressWarnings("resource")
    protected void before() throws Throwable {
        try {
            Settings settings = Settings.builder()
                    .put("client.transport.ignore_cluster_name", true)
                    .build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(), 9300));
        } catch (ExceptionInInitializerError e) {
            if (e.getCause() instanceof AccessControlException) {
                throw new RuntimeException(getClass().getSimpleName() + " is not available with the security manager", e);
            } else {
                throw e;
            }
        }
        server = new JdbcHttpServer(client);

        server.start(0);
        jdbcUrl = server.url();
    }

    @Override
    protected void after() {
        client.close();
        client = null;
        server.stop();
        server = null;
    }

    @Override
    public Connection get() throws SQLException {
        assertNotNull("ES JDBC Server is null - make sure ES is properly run as a @ClassRule", server);
        return DriverManager.getConnection(jdbcUrl, properties);
    }
}