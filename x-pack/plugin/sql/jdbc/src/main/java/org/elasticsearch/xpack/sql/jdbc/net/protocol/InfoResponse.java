/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

/**
 * General information about the server.
 */
public class InfoResponse {
    public final String cluster;
    public final int majorVersion;
    public final int minorVersion;

    public InfoResponse(String clusterName, byte versionMajor, byte versionMinor) {
        this.cluster = clusterName;
        this.majorVersion = versionMajor;
        this.minorVersion = versionMinor;
    }
}