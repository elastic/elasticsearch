/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbcx;

import org.elasticsearch.xpack.sql.jdbc.debug.Debug;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConfiguration;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConnection;

import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Wrapper;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.sql.DataSource;

public class JdbcDataSource implements DataSource, Wrapper, Closeable {

    private String url;
    private PrintWriter writer;
    private int loginTimeout;
    private Properties info;

    public JdbcDataSource() {}

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return writer;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.writer = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Properties getInfo() {
        return info;
    }

    public void setInfo(Properties props) {
        this.info = props;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return doGetConnection(info);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        // TODO: set user/pass props
        throw new UnsupportedOperationException();
        //return doGetConnection(null);
    }

    private Connection doGetConnection(Properties p) {
        JdbcConfiguration ci = new JdbcConfiguration(url, p);
        if (loginTimeout > 0) {
            ci.connectTimeout(TimeUnit.SECONDS.toMillis(loginTimeout));
        }
        return new JdbcConnection(ci);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException();
    }

    @Override
    public void close() {
        Debug.close();
    }
}