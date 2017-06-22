/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.util;

import org.elasticsearch.common.CheckedSupplier;
import org.junit.rules.ExternalResource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class H2 extends ExternalResource implements CheckedSupplier<Connection, SQLException> {
    static {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private final String url;
    private final Properties DEFAULT_PROPS = new Properties();
    private Connection keepAlive;

    public H2() {
        this(null);
    }

    public H2(String db) {
        this.url = (db == null ? "jdbc:h2:mem:essql" : db) + ";DATABASE_TO_UPPER=false;ALIAS_COLUMN_NAME=true";

        if (db != null) {
            DEFAULT_PROPS.setProperty("user", "sa");
        }
    }

    @Override
    protected void before() throws Throwable {
        keepAlive = get();
    }

    @Override
    protected void after() {
        try {
            keepAlive.close();
        } catch (SQLException e) {
            // ignore
        }
        keepAlive = null;
    }

    @Override
    public Connection get() throws SQLException {
        return DriverManager.getConnection(url, DEFAULT_PROPS);
    }
}