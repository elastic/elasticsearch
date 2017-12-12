/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.ResponseType;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractInfoResponse;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

import java.io.DataInput;
import java.io.IOException;

/**
 * General information about the server.
 */
public class InfoResponse extends AbstractInfoResponse {
    public InfoResponse(String nodeName, String clusterName, byte versionMajor, byte versionMinor, String version,
            String versionHash, String versionDate) {
        super(nodeName, clusterName, versionMajor, versionMinor, version, versionHash, versionDate);
    }

    InfoResponse(Request request, DataInput in) throws IOException {
        super(request, in);
    }

    @Override
    public RequestType requestType() {
        return RequestType.INFO;
    }

    @Override
    public ResponseType responseType() {
        return ResponseType.INFO;
    }
}