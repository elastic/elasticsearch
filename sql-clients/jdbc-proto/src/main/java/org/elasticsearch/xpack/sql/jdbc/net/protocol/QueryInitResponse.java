/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Status;

import static java.lang.String.format;
import static java.util.Collections.emptyList;

public class QueryInitResponse extends DataResponse {

    public final long serverTimeQueryReceived, serverTimeResponseSent, timeSpent;
    public final String requestId;
    public final List<ColumnInfo> columns;

    public QueryInitResponse(long serverTimeQueryReceived, long serverTimeResponseSent, String requestId, List<ColumnInfo> columns, Object data) {
        super(Action.QUERY_INIT, data);
        this.serverTimeQueryReceived = serverTimeQueryReceived;
        this.serverTimeResponseSent = serverTimeResponseSent;
        this.timeSpent = serverTimeQueryReceived - serverTimeResponseSent;
        this.requestId = requestId;
        this.columns = columns;
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "QueryInitRes[%s]", requestId);
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(Status.toSuccess(action));

        out.writeLong(serverTimeQueryReceived);
        out.writeLong(serverTimeResponseSent);
        out.writeUTF(requestId);

        out.writeInt(columns.size());
        for (ColumnInfo c : columns) {
            out.writeUTF(c.name);
            out.writeUTF(c.label);
            out.writeUTF(c.table);
            out.writeUTF(c.schema);
            out.writeUTF(c.catalog);
            out.writeInt(c.type);
        }
    }

    public static QueryInitResponse decode(DataInput in) throws IOException {
        long serverTimeQueryReceived = in.readLong();
        long serverTimeResponseSent = in.readLong();
        String requestId = in.readUTF();


        int length = in.readInt();
        List<ColumnInfo> list = length < 1 ? emptyList() : new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            String name = in.readUTF();
            String label = in.readUTF();
            String table = in.readUTF();
            String schema = in.readUTF();
            String catalog = in.readUTF();
            int type = in.readInt();

            list.add(new ColumnInfo(name, type, schema, catalog, table, label));
        }

        return new QueryInitResponse(serverTimeQueryReceived, serverTimeResponseSent, requestId, list, null);
    }
}