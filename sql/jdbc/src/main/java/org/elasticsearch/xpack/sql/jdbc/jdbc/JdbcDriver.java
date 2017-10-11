/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import org.elasticsearch.xpack.sql.jdbc.debug.Debug;
import org.elasticsearch.xpack.sql.jdbc.util.Version;

import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class JdbcDriver implements java.sql.Driver, Closeable {

    private static final JdbcDriver INSTANCE = new JdbcDriver();

    static {
        register();
    }

    public static JdbcDriver register() {
        try {
            DriverManager.registerDriver(INSTANCE, INSTANCE::close);
        } catch (SQLException ex) {
            // the SQLException is bogus as there's no source for it
            PrintWriter writer = DriverManager.getLogWriter();
            if (writer != null) {
                ex.printStackTrace(writer);
            }
        }
        return INSTANCE;
    }

    public static void deregister() {
        try {
            DriverManager.deregisterDriver(INSTANCE);
        } catch (SQLException ex) {
            // the SQLException is bogus as there's no source for it
            PrintWriter writer = DriverManager.getLogWriter();
            if (writer != null) {
                ex.printStackTrace(writer);
            }
        }
    }

    public static int jdbcMajorVersion() {
        return 4;
    }

    public static int jdbcMinorVersion() {
        return 2;
    }

    //
    // Jdbc 4.0
    //
    public Connection connect(String url, Properties props) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        JdbcConfiguration cfg = initCfg(url, props);
        JdbcConnection con = new JdbcConnection(cfg);
        return cfg.debug() ? Debug.proxy(cfg, con, DriverManager.getLogWriter()) : con;
    }

    private static JdbcConfiguration initCfg(String url, Properties props) {
        JdbcConfiguration ci = new JdbcConfiguration(url, props);

        // if there's a timeout set on the DriverManager, make sure to use it
        if (DriverManager.getLoginTimeout() > 0) {
            ci.connectTimeout(TimeUnit.SECONDS.toMillis(DriverManager.getLoginTimeout()));
        }
        return ci;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return JdbcConfiguration.canAccept(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return new DriverPropertyInfo[0];
        }
        return new JdbcConfiguration(url, info).driverPropertyInfo();
    }

    @Override
    public int getMajorVersion() {
        return Version.versionMajor();
    }

    @Override
    public int getMinorVersion() {
        return Version.versionMinor();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    //
    // Jdbc 4.1
    //

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void close() {
        // TODO: clean-up resources
        Debug.close();
    }
}