/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.cli.CliHttpClient;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryInitRequest;

/**
 * Stores information about the current session
 */
public class CliSession {
    private final CliHttpClient cliHttpClient;
    private int fetchSize = AbstractQueryInitRequest.DEFAULT_FETCH_SIZE;
    private String fetchSeparator = "";
    private boolean debug;

    public CliSession(CliHttpClient cliHttpClient) {
        this.cliHttpClient =  cliHttpClient;
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
}
