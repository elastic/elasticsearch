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
    final int revisionVersion;

    InfoResponse(String clusterName, byte versionMajor, byte versionMinor, byte revisionVersion) {
        this.cluster = clusterName;
        this.majorVersion = versionMajor;
        this.minorVersion = versionMinor;
        this.revisionVersion = revisionVersion;
    }

    @Override
    public String toString() {
        return cluster + "[" + versionString() + "]";
    }
    
    public String versionString() {
        return majorVersion + "." + minorVersion + "." + revisionVersion;
    }
}