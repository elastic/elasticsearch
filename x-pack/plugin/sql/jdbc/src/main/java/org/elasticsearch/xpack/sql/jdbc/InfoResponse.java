/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;


import org.elasticsearch.xpack.sql.proto.SqlVersion;

/**
 * General information about the server.
 */
class InfoResponse {
    final String cluster;
    final SqlVersion version;

    InfoResponse(String clusterName, SqlVersion version) {
        this.cluster = clusterName;
        this.version = version;
    }

    @Override
    public String toString() {
        return cluster + "[" + version.toString() + "]";
    }
}