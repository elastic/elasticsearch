/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster;

public enum LogType {
    SERVER("%s.log"),
    SERVER_JSON("%s_server.json"),
    AUDIT("%s_audit.json"),
    SEARCH_SLOW("%s_index_search_slowlog.json"),
    INDEXING_SLOW("%s_index_indexing_slowlog.json"),
    ESQL_QUERY("%s_esql_querylog.json"),
    DEPRECATION("%s_deprecation.json");

    private final String filenameFormat;

    LogType(String filenameFormat) {
        this.filenameFormat = filenameFormat;
    }

    public String resolveFilename(String clusterName) {
        return filenameFormat.formatted(clusterName);
    }
}
