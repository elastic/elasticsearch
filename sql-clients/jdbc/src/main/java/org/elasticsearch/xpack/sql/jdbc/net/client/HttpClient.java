/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.client;

import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConfiguration;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcException;
import org.elasticsearch.xpack.sql.jdbc.util.BytesArray;
import org.elasticsearch.xpack.sql.net.client.ClientException;
import org.elasticsearch.xpack.sql.net.client.DataOutputConsumer;
import org.elasticsearch.xpack.sql.net.client.jre.JreHttpUrlConnection;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;

// http client
// handles nodes discovery, fail-over, errors, etc...
class HttpClient {

    private final JdbcConfiguration cfg;
    private final URL baseUrl;

    HttpClient(JdbcConfiguration connectionInfo) {
        this.cfg = connectionInfo;
        baseUrl = connectionInfo.asUrl();
    }

    void setNetworkTimeout(long millis) {
        cfg.setNetworkTimeout(millis);
    }

    long getNetworkTimeout() {
        return cfg.getNetworkTimeout();
    }

    private URL url(String subPath) {
        try {
            return new URL(baseUrl, subPath);
        } catch (MalformedURLException ex) {
            throw new JdbcException(ex, "Invalid subpath %s", subPath);
        }
    }

    boolean head(String path) { // NOCOMMIT remove path?
        try {
            return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
                return JreHttpUrlConnection.http(url(path), cfg, JreHttpUrlConnection::head);
            });
        } catch (ClientException ex) {
            throw new JdbcException(ex, "Transport failure");
        }
    }

    BytesArray put(DataOutputConsumer os) throws SQLException {
        return put("_jdbc?error_trace=true", os); // NOCOMMIT Do something with the error trace. Useful for filing bugs and debugging.
    }

    BytesArray put(String path, DataOutputConsumer os) throws SQLException { // NOCOMMIT remove path?
        try {
            return AccessController.doPrivileged((PrivilegedAction<BytesArray>) () -> {    
                return JreHttpUrlConnection.http(url(path), cfg, con -> {
                    return new BytesArray(con.put(os));
                });
            });
        } catch (ClientException ex) {
            throw new JdbcException(ex, "Transport failure");
        }
    }

    void close() {}
}