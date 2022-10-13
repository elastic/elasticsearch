/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.client.ClientException;
import org.elasticsearch.xpack.sql.client.ClientVersion;
import org.elasticsearch.xpack.sql.client.HttpClient;
import org.elasticsearch.xpack.sql.proto.MainResponse;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import java.sql.SQLException;

/**
 * Stores information about the current session
 */
public class CliSession {
    private final HttpClient httpClient;
    private final CliSessionConfiguration configuration;

    public CliSession(HttpClient httpClient) {
        this.httpClient = httpClient;
        this.configuration = new CliSessionConfiguration();
    }

    public HttpClient getClient() {
        return httpClient;
    }

    public CliSessionConfiguration cfg() {
        return configuration;
    }

    public void checkConnection() throws ClientException {
        MainResponse response;
        try {
            response = httpClient.serverInfo();
        } catch (SQLException ex) {
            throw new ClientException(ex);
        }
        SqlVersion version = SqlVersion.fromString(response.getVersion());
        if (ClientVersion.isServerCompatible(version) == false) {
            throw new ClientException(
                "This version of the CLI is only compatible with Elasticsearch version "
                    + ClientVersion.CURRENT.majorMinorToString()
                    + " or newer; attempting to connect to a server version "
                    + version.toString()
            );
        }
    }
}
