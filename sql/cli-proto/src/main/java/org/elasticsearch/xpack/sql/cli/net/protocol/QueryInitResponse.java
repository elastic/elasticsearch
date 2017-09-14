/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.ResponseType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

import java.io.DataInput;
import java.io.IOException;

public class QueryInitResponse extends QueryResponse {
    public QueryInitResponse(long tookNanos, byte[] cursor, String data) {
        super(tookNanos, cursor, data);
    }

    QueryInitResponse(Request request, DataInput in) throws IOException {
        super(request, in);
    }

    @Override
    public RequestType requestType() {
        return RequestType.QUERY_INIT;
    }

    @Override
    public ResponseType responseType() {
        return ResponseType.QUERY_INIT;
    }
}
