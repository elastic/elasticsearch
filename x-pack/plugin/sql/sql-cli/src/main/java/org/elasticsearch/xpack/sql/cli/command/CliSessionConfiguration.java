/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.proto.CoreProtocol;

/**
 * Configuration for CLI session
 */
public class CliSessionConfiguration {
    private int fetchSize;
    private String fetchSeparator = "";
    private boolean debug;
    private boolean lenient;
    private boolean allowPartialResults;

    public CliSessionConfiguration() {
        this.fetchSize = CoreProtocol.FETCH_SIZE;
        this.lenient = CoreProtocol.FIELD_MULTI_VALUE_LENIENCY;
        this.allowPartialResults = CoreProtocol.ALLOW_PARTIAL_SEARCH_RESULTS;
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

    public boolean isLenient() {
        return lenient;
    }

    public void setLenient(boolean lenient) {
        this.lenient = lenient;
    }

    public boolean allowPartialResults() {
        return allowPartialResults;
    }

    public void setAllowPartialResults(boolean allowPartialResults) {
        this.allowPartialResults = allowPartialResults;
    }
}
