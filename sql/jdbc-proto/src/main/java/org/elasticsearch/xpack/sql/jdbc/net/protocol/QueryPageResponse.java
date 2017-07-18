/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.ResponseType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class QueryPageResponse extends Response {
    public final String requestId;
    private final Payload data;

    public QueryPageResponse(String requestId, Page data) {
        if (requestId == null) {
            throw new IllegalArgumentException("[requestId] must not be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("[data] must not be null");
        }
        this.requestId = requestId;
        this.data = data;
    }

    QueryPageResponse(Request request, DataInput in) throws IOException {
        this.requestId = in.readUTF();
        QueryPageRequest queryPageRequest = (QueryPageRequest) request;
        data = queryPageRequest.data();
        queryPageRequest.data().read(in);
    }

    @Override
    public void write(int clientVersion, DataOutput out) throws IOException {
        out.writeUTF(requestId);
        data.write(out);
    }

    @Override
    protected String toStringBody() {
        return "requestId=[" + requestId
                + "] data=[\n" + data + "]";
    }

    @Override
    public RequestType requestType() {
        return RequestType.QUERY_PAGE;
    }

    @Override
    public ResponseType responseType() {
        return ResponseType.QUERY_PAGE;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        QueryPageResponse other = (QueryPageResponse) obj;
        return requestId.equals(other.requestId)
                && data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, data);
    }
}
