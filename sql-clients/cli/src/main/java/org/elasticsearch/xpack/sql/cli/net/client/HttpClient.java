/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.client;

import java.net.MalformedURLException;
import java.net.URL;

import org.elasticsearch.xpack.sql.cli.CliConfiguration;
import org.elasticsearch.xpack.sql.cli.CliException;
import org.elasticsearch.xpack.sql.net.client.ClientException;
import org.elasticsearch.xpack.sql.net.client.DataOutputConsumer;
import org.elasticsearch.xpack.sql.net.client.jre.JreHttpUrlConnection;
import org.elasticsearch.xpack.sql.net.client.util.Bytes;

public class HttpClient {

    private final CliConfiguration cfg;

    public HttpClient(CliConfiguration cfg) {
        this.cfg = cfg;
    }

    private URL url(String subPath) {
        try {
            return new URL(cfg.asUrl(), subPath);
        } catch (MalformedURLException ex) {
            throw new ClientException(ex, "Invalid subpath %s", subPath);
        }
    }

    boolean head(String path) {
        try {
            return JreHttpUrlConnection.http(url(path), cfg, JreHttpUrlConnection::head);
        } catch (ClientException ex) {
            throw new ClientException(ex, "Transport failure");
        }
    }

    Bytes put(DataOutputConsumer os) {
        return put("sql/", os);
    }

    Bytes put(String path, DataOutputConsumer os) {
        try {
            return JreHttpUrlConnection.http(url(path), cfg, con -> {
                return con.put(os);
            });
        } catch (ClientException ex) {
            throw new CliException(ex, "Transport failure");
        }
    }

    void close() {}
}