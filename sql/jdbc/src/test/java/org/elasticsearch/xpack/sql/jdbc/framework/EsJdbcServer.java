/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcDriver;
import org.junit.rules.ExternalResource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

public class EsJdbcServer extends ExternalResource implements CheckedSupplier<Connection, SQLException> {

    private JdbcHttpServer server;
    private String jdbcUrl;
    private JdbcDriver driver;
    private final Properties properties;

    public EsJdbcServer() {
        this(false);
    }

    public EsJdbcServer(boolean debug) {
        properties = new Properties();
        if (debug) {
            properties.setProperty("debug", "true");
        }
    }

    @Override
    protected void before() throws Throwable {
        server = new JdbcHttpServer(TestUtils.client());
        driver = new JdbcDriver();

        server.start(0);
        jdbcUrl = server.url();

        System.out.println("Started JDBC Server at " + jdbcUrl);
    }

    @Override
    protected void after() {
        server.stop();
        server = null;

        System.out.println("Stopped JDBC Server at " + jdbcUrl);
    }

    public Client client() {
        assertNotNull("ES JDBC Server is null - make sure ES is properly run as a @ClassRule", driver);
        return server.client();
    }

    @Override
    public Connection get() throws SQLException {
        assertNotNull("ES JDBC Server is null - make sure ES is properly run as a @ClassRule", driver);
        return driver.connect(jdbcUrl, properties);
    }
}