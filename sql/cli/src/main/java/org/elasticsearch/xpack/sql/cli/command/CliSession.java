/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.cli.CliHttpClient;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.client.shared.ClientException;
import org.elasticsearch.xpack.sql.client.shared.Version;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryInitRequest;

import java.sql.SQLException;

/**
 * Stores information about the current session
 */
public class CliSession {
    private final CliHttpClient cliHttpClient;
    private int fetchSize = AbstractQueryInitRequest.DEFAULT_FETCH_SIZE;
    private String fetchSeparator = "";
    private boolean debug;

    public CliSession(CliHttpClient cliHttpClient) {
        this.cliHttpClient = cliHttpClient;
    }

    public CliHttpClient getClient() {
        return cliHttpClient;
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
        InfoResponse response;
        try {
            response = cliHttpClient.serverInfo();
        } catch (SQLException ex) {
            throw new ClientException(ex);
        }
        // TODO: We can relax compatibility requirement later when we have a better idea about protocol compatibility guarantees
        if (response.majorVersion != Version.versionMajor() || response.minorVersion != Version.versionMinor()) {
            throw new ClientException("This alpha version of CLI is only compatible with Elasticsearch version " + Version.version());
        }
    }
}
