/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.client;

import org.elasticsearch.xpack.sql.jdbc.JdbcException;
import org.elasticsearch.xpack.sql.jdbc.JdbcSQLException;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConfiguration;
import org.elasticsearch.xpack.sql.jdbc.util.BytesArray;
import org.elasticsearch.xpack.sql.net.client.ClientException;
import org.elasticsearch.xpack.sql.net.client.JreHttpUrlConnection;
import org.elasticsearch.xpack.sql.net.client.util.CheckedConsumer;

import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;

// http client
// handles nodes discovery, fail-over, errors, etc...
class HttpClient {

    private final JdbcConfiguration cfg;
    private final URL url;

    HttpClient(JdbcConfiguration connectionInfo) throws SQLException {
        this.cfg = connectionInfo;
        URL baseUrl = connectionInfo.asUrl();
        try {
            // the baseUrl ends up / so the suffix can be directly appended
            // NOCOMMIT Do something with the error trace. Useful for filing bugs and debugging.
            this.url = new URL(baseUrl, "_sql/jdbc?error_trace=true");
        } catch (MalformedURLException ex) {
            throw new JdbcException(ex, "Cannot connect to JDBC endpoint [" + baseUrl.toString() + "_sql/jdbc]");
    }
    }

    void setNetworkTimeout(long millis) {
        cfg.networkTimeout(millis);
    }

    long getNetworkTimeout() {
        return cfg.networkTimeout();
    }

    boolean head() throws JdbcSQLException {
        try {
            URL root = new URL(url, "/");
            return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
                return JreHttpUrlConnection.http(root, cfg, JreHttpUrlConnection::head);
            });
        } catch (MalformedURLException ex) {
            throw new JdbcSQLException(ex, "Cannot ping server");
        } catch (ClientException ex) {
            throw new JdbcSQLException(ex, "Transport failure");
        }
    }

    BytesArray put(CheckedConsumer<DataOutput, IOException> os) throws SQLException {
        try {
            return AccessController.doPrivileged((PrivilegedAction<BytesArray>) () -> {    
                return JreHttpUrlConnection.http(url, cfg, con -> {
                    return new BytesArray(con.post(os));
                });
            });
        } catch (ClientException ex) {
            throw new JdbcSQLException(ex, "Transport failure");
        }
    }

    void close() {}
}