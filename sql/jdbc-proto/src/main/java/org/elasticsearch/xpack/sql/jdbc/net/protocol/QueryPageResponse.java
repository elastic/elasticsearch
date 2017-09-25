/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.ResponseType;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryResponse;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.SqlDataInput;
import org.elasticsearch.xpack.sql.protocol.shared.SqlDataOutput;

import java.io.IOException;
import java.util.Objects;

public class QueryPageResponse extends AbstractQueryResponse {
    private final Payload data;

    public QueryPageResponse(long tookNanos, byte[] cursor, Payload data) {
        super(tookNanos, cursor);
        this.data = data;
    }

    QueryPageResponse(Request request, SqlDataInput in) throws IOException {
        super(request, in);
        QueryPageRequest queryPageRequest = (QueryPageRequest) request;
        data = queryPageRequest.data();
        queryPageRequest.data().readFrom(in);
    }

    @Override
    public void writeTo(SqlDataOutput out) throws IOException {
        super.writeTo(out);
        data.writeTo(out);
    }

    @Override
    protected String toStringBody() {
        return super.toStringBody() + " data=[\n" + data + "]";
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
        if (false == super.equals(obj)) {
            return false;
        }
        QueryPageResponse other = (QueryPageResponse) obj;
        return data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
