/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.elasticsearch.xpack.sql.jdbc.debug.Debug;
import org.elasticsearch.xpack.sql.jdbc.util.Version;

public class JdbcDriver implements java.sql.Driver, Closeable {

    static {
        try {
            final JdbcDriver d = new JdbcDriver();
            DriverManager.registerDriver(d, d::close);
        } catch (Exception ex) {
            // NOCOMMIT this seems bad!
            // ignore
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

        JdbcConfiguration info = initInfo(url, props);
        JdbcConnection con = new JdbcConnection(info);
        return info.debug() ? Debug.proxy(info, con, DriverManager.getLogWriter()) : con;
    }

    private static JdbcConfiguration initInfo(String url, Properties props) {
        JdbcConfiguration ci = new JdbcConfiguration(url, props);
        if (DriverManager.getLoginTimeout() > 0) {
            ci.setConnectTimeout(TimeUnit.SECONDS.toMillis(DriverManager.getLoginTimeout()));
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