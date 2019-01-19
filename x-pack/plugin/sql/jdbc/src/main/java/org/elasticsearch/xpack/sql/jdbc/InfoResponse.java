/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

/**
 * General information about the server.
 */
class InfoResponse {
    final String cluster;
    final int majorVersion;
    final int minorVersion;

    InfoResponse(String clusterName, byte versionMajor, byte versionMinor) {
        this.cluster = clusterName;
        this.majorVersion = versionMajor;
        this.minorVersion = versionMinor;
    }
}