/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
/*
\ * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.client;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;

import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConfiguration;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcException;
import org.elasticsearch.xpack.sql.jdbc.util.BytesArray;
import org.elasticsearch.xpack.sql.net.client.ClientException;
import org.elasticsearch.xpack.sql.net.client.DataOutputConsumer;
import org.elasticsearch.xpack.sql.net.client.jre.JreHttpUrlConnection;

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

    boolean head(String path) {
        try {
            return JreHttpUrlConnection.http(url(path), cfg, JreHttpUrlConnection::head);
        } catch (ClientException ex) {
            throw new JdbcException(ex, "Transport failure");
        }
    }

    BytesArray put(DataOutputConsumer os) throws SQLException {
        return put("sql/", os);
    }

    BytesArray put(String path, DataOutputConsumer os) throws SQLException {
        try {
            return JreHttpUrlConnection.http(url(path), cfg, con -> {
                return new BytesArray(con.put(os));
            });
        } catch (ClientException ex) {
            throw new JdbcException(ex, "Transport failure");
        }
    }

    void close() {}
}