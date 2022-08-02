/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.xpack.sql.client.ClientVersion;
import org.elasticsearch.xpack.sql.client.ConnectionConfiguration;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Wrapper;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * Factory for connections to Elasticsearch SQL.
 */
public class EsDataSource implements DataSource, Wrapper {

    static {
        // invoke Version to perform classpath/jar sanity checks
        ClientVersion.CURRENT.toString();
    }

    private String url;
    private PrintWriter writer;
    private int loginTimeout;
    private Properties props;

    public EsDataSource() {}

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
        if (seconds < 0) {
            throw new SQLException("Negative timeout specified " + seconds);
        }
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

    public Properties getProperties() {
        Properties copy = new Properties();
        if (props != null) {
            copy.putAll(props);
        }
        return copy;
    }

    public void setProperties(Properties props) {
        this.props = new Properties();
        this.props.putAll(props);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return doGetConnection(getProperties());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Properties p = getProperties();
        p.setProperty(ConnectionConfiguration.AUTH_USER, username);
        p.setProperty(ConnectionConfiguration.AUTH_PASS, password);
        return doGetConnection(p);
    }

    private Connection doGetConnection(Properties p) throws SQLException {
        JdbcConfiguration cfg = JdbcConfiguration.create(url, p, loginTimeout);
        JdbcConnection con = new JdbcConnection(cfg);
        // enable logging if needed
        return cfg.debug() ? Debug.proxy(cfg, con, writer) : con;
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
}
