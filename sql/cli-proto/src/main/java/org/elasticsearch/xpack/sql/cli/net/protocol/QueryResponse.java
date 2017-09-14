/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryResponse;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public abstract class QueryResponse extends AbstractQueryResponse {
    public final String data;

    protected QueryResponse(long tookNanos, byte[] cursor, String data) {
        super(tookNanos, cursor);
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        this.data = data;
    }

    protected QueryResponse(Request request, DataInput in) throws IOException {
        super(request, in);
        data = in.readUTF();
    }

    @Override
    protected void write(int clientVersion, DataOutput out) throws IOException {
        super.write(clientVersion, out);
        out.writeUTF(data);
    }

    @Override
    protected String toStringBody() {
        return super.toStringBody() + " data=[" + data + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        QueryResponse other = (QueryResponse) obj;
        return data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), data);
    }
}
