/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.client.HttpClient;
import org.elasticsearch.xpack.sql.client.ClientException;
import org.elasticsearch.xpack.sql.client.Version;
import org.elasticsearch.xpack.sql.proto.MainResponse;
import org.elasticsearch.xpack.sql.proto.Protocol;

import java.sql.SQLException;

/**
 * Stores information about the current session
 */
public class CliSession {
    private final HttpClient httpClient;
    private int fetchSize = Protocol.FETCH_SIZE;
    private String fetchSeparator = "";
    private boolean debug;

    public CliSession(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public HttpClient getClient() {
        return httpClient;
    }

    public void setFetchSize(int fetchSize) {
        if (fetchSize <= 0) {
            throw new IllegalArgumentException("Must be > 0.");
        }
        this.fetchSize = fetchSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSeparator(String fetchSeparator) {
        this.fetchSeparator = fetchSeparator;
    }

    public String getFetchSeparator() {
        return fetchSeparator;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public boolean isDebug() {
        return debug;
    }

    public void checkConnection() throws ClientException {
        MainResponse response;
        try {
            response = httpClient.serverInfo();
        } catch (SQLException ex) {
            throw new ClientException(ex);
        }
        Version version = Version.fromString(response.getVersion());
        // TODO: We can relax compatibility requirement later when we have a better idea about protocol compatibility guarantees
        if (version.major != Version.CURRENT.major || version.minor != Version.CURRENT.minor) {
            throw new ClientException("This alpha version of CLI is only compatible with Elasticsearch version " +
                    Version.CURRENT.toString());
        }
    }
}
