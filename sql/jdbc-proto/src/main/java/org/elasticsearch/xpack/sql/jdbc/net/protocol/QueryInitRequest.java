/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.TimeZone;

public class QueryInitRequest extends Request {
    public final int fetchSize;
    public final String query;
    public final TimeZone timeZone;
    public final TimeoutInfo timeout;

    public QueryInitRequest(int fetchSize, String query, TimeZone timeZone, TimeoutInfo timeout) {
        this.fetchSize = fetchSize;
        this.query = query;
        this.timeZone = timeZone;
        this.timeout = timeout;
    }

    QueryInitRequest(int clientVersion, DataInput in) throws IOException {
        fetchSize = in.readInt();
        query = in.readUTF();
        timeZone = TimeZone.getTimeZone(in.readUTF());
        timeout = new TimeoutInfo(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(fetchSize);
        out.writeUTF(query);
        out.writeUTF(timeZone.getID());
        timeout.write(out);
    }

    @Override
    protected String toStringBody() {
        StringBuilder b = new StringBuilder();
        b.append("query=[").append(query).append(']');
        if (false == timeZone.getID().equals("UTC")) {
            b.append(" timeZone=[").append(timeZone.getID()).append(']');
        }
        return b.toString();
    }

    @Override
    public RequestType requestType() {
        return RequestType.QUERY_INIT;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        QueryInitRequest other = (QueryInitRequest) obj;
        return fetchSize == other.fetchSize
                && Objects.equals(query, other.query)
                && Objects.equals(timeout, other.timeout)
                && Objects.equals(timeZone.getID(), other.timeZone.getID());
    }

    @Override
    public int hashCode() {
        return Objects.hash(fetchSize, query, timeout, timeZone.getID().hashCode());
    }
}