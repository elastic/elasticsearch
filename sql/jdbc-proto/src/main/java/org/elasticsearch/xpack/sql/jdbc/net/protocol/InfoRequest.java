/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractInfoRequest;

import java.io.DataInput;
import java.io.IOException;

/**
 * Request general information about the server.
 */
public class InfoRequest extends AbstractInfoRequest {
    /**
     * Build the info request containing information about the current JVM.
     */
    public InfoRequest() {
        super();
    }

    public InfoRequest(String jvmVersion, String jvmVendor, String jvmClassPath, String osName, String osVersion) {
        super(jvmVersion, jvmVendor, jvmClassPath, osName, osVersion);
    }

    InfoRequest(int clientVersion, DataInput in) throws IOException {
        super(clientVersion, in);
    }

    @Override
    public RequestType requestType() {
        return RequestType.INFO;
    }
}
