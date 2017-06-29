/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.client;

import org.elasticsearch.xpack.sql.cli.CliConfiguration;
import org.elasticsearch.xpack.sql.net.client.ClientException;
import org.elasticsearch.xpack.sql.net.client.DataOutputConsumer;
import org.elasticsearch.xpack.sql.net.client.jre.JreHttpUrlConnection;
import org.elasticsearch.xpack.sql.net.client.util.Bytes;

import java.net.MalformedURLException;
import java.net.URL;

class HttpClient {

    private final CliConfiguration cfg;

    HttpClient(CliConfiguration cfg) {
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
        return JreHttpUrlConnection.http(url(path), cfg, JreHttpUrlConnection::head);
    }

    Bytes put(DataOutputConsumer os) {
        return put("", os);
    }

    Bytes put(String path, DataOutputConsumer os) {
        return JreHttpUrlConnection.http(url(path), cfg, con -> {
            return con.put(os);
        });
    }

    void close() {}
}