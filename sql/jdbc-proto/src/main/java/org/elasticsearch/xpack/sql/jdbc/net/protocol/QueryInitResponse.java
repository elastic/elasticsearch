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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

public class QueryInitResponse extends Response {
    public final long serverTimeQueryReceived, serverTimeResponseSent;
    public final String requestId;
    public final List<ColumnInfo> columns;
    public final ResultPage data;

    public QueryInitResponse(long serverTimeQueryReceived, long serverTimeResponseSent, String requestId, List<ColumnInfo> columns,
            ResultPage data) {
        this.serverTimeQueryReceived = serverTimeQueryReceived;
        this.serverTimeResponseSent = serverTimeResponseSent;
        this.requestId = requestId;
        this.columns = columns;
        this.data = data;
    }

    QueryInitResponse(Request request, DataInput in) throws IOException {
        serverTimeQueryReceived = in.readLong();
        serverTimeResponseSent = in.readLong();
        requestId = in.readUTF();
        int size = in.readInt();
        List<ColumnInfo> columns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            columns.add(new ColumnInfo(in));
        }
        this.columns = unmodifiableList(columns);
        Page data = new Page(columns);
        data.read(in);
        this.data = data;
    }

    @Override
    public void write(int clientVersion, DataOutput out) throws IOException {
        out.writeLong(serverTimeQueryReceived);
        out.writeLong(serverTimeResponseSent);
        out.writeUTF(requestId);
        out.writeInt(columns.size());
        for (ColumnInfo c : columns) {
            c.write(out);
        }
        data.write(out);
    }

    @Override
    protected String toStringBody() {
        return "timeReceived=[" + serverTimeQueryReceived
                + "] timeSent=[" + serverTimeResponseSent
                + "] requestId=[" + requestId
                + "] columns=" + columns
                + " data=[\n" + data + "]";
    }

    @Override
    public RequestType requestType() {
        return RequestType.QUERY_INIT;
    }

    @Override
    public ResponseType responseType() {
        return ResponseType.QUERY_INIT;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        QueryInitResponse other = (QueryInitResponse) obj;
        return serverTimeQueryReceived == other.serverTimeQueryReceived
                && serverTimeResponseSent == other.serverTimeResponseSent
                && requestId.equals(other.requestId)
                && columns.equals(other.columns);
        // NOCOMMIT data
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverTimeQueryReceived, serverTimeResponseSent, requestId, columns); // NOCOMMIT data
    }
}