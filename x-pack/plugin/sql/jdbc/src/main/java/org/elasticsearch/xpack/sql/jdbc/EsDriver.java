/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.xpack.sql.client.ClientVersion;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class EsDriver implements Driver {

    private static final EsDriver INSTANCE = new EsDriver();

    static {
        // invoke Version to perform classpath/jar sanity checks
        ClientVersion.CURRENT.toString();

        try {
            register();
        } catch (SQLException ex) {
            // the SQLException is bogus as there's no source for it
            // but we handle it just in case
            PrintWriter writer = DriverManager.getLogWriter();
            if (writer != null) {
                ex.printStackTrace(writer);
                writer.flush();
            }
            throw new ExceptionInInitializerError(ex);
        }
    }

    public static EsDriver register() throws SQLException {
        // no closing callback
        DriverManager.registerDriver(INSTANCE, INSTANCE::close);
        return INSTANCE;
    }

    public static void deregister() throws SQLException {
        try {
            DriverManager.deregisterDriver(INSTANCE);
        } catch (SQLException ex) {
            // the SQLException is bogus as there's no source for it
            // but we handle it just in case
            PrintWriter writer = DriverManager.getLogWriter();
            if (writer != null) {
                ex.printStackTrace(writer);
                writer.flush();
            }
            throw ex;
        }
    }

    //
    // Jdbc 4.0
    //
    @Override
    public Connection connect(String url, Properties props) throws SQLException {
        if (url == null) {
            throw new JdbcSQLException("Non-null url required");
        }
        if (acceptsURL(url) == false) {
            return null;
        }

        JdbcConfiguration cfg = initCfg(url, props);
        JdbcConnection con = new JdbcConnection(cfg);
        return cfg.debug() ? Debug.proxy(cfg, con, DriverManager.getLogWriter()) : con;
    }

    private static JdbcConfiguration initCfg(String url, Properties props) throws JdbcSQLException {
        return JdbcConfiguration.create(url, props, DriverManager.getLoginTimeout());
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return JdbcConfiguration.canAccept(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (acceptsURL(url) == false) {
            return new DriverPropertyInfo[0];
        }
        return JdbcConfiguration.create(url, info, DriverManager.getLoginTimeout()).driverPropertyInfo();
    }

    @Override
    public int getMajorVersion() {
        return ClientVersion.CURRENT.major;
    }

    @Override
    public int getMinorVersion() {
        return ClientVersion.CURRENT.minor;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    //
    // Jdbc 4.1
    //

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Cleanup method invoked by the DriverManager when unregistering the driver.
     * Since this happens typically when the JDBC driver gets unloaded (from the classloader)
     * cleaning all debug information is a good safety check.
     */
    private void close() {
        Debug.close();
    }
}
