/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.client;

import org.elasticsearch.xpack.sql.cli.CliConfiguration;
import org.elasticsearch.xpack.sql.net.client.ClientException;
import org.elasticsearch.xpack.sql.net.client.JreHttpUrlConnection;
import org.elasticsearch.xpack.sql.net.client.util.Bytes;
import org.elasticsearch.xpack.sql.net.client.util.CheckedConsumer;

import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;

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
        try {
            return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
                return JreHttpUrlConnection.http(url(path), cfg, JreHttpUrlConnection::head);
            });
        } catch (ClientException ex) {
            throw new RuntimeException("Transport failure", ex);
        }
    }

    Bytes put(CheckedConsumer<DataOutput, IOException> os) {
        return put("", os);
    }

    Bytes put(String path, CheckedConsumer<DataOutput, IOException> os) {
        try {
            return AccessController.doPrivileged((PrivilegedAction<Bytes>) () -> {
                return JreHttpUrlConnection.http(url(path), cfg, con -> {
                    return con.put(os);
                });
            });
        } catch (ClientException ex) {
            throw new RuntimeException("Transport failure", ex);
        }
    }

    void close() {}
}