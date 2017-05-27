/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Action;

import static java.lang.String.format;

public class QueryInitRequest extends Request {

    public final int fetchSize;
    public final String query;
    public final TimeoutInfo timeout;

    public QueryInitRequest(int fetchSize, String query, TimeoutInfo timeout) {
        super(Action.QUERY_INIT);
        this.fetchSize = fetchSize;
        this.query = query;
        this.timeout = timeout;
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(action.value());
        out.writeInt(fetchSize);
        timeout.encode(out);
        out.writeUTF(query);
    }

    public static QueryInitRequest decode(DataInput in) throws IOException {
        int fetchSize = in.readInt();
        TimeoutInfo timeout = TimeoutInfo.readTimeout(in);
        String query = in.readUTF();

        return new QueryInitRequest(fetchSize, query, timeout);
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "SqlInitReq[%s]", query);
    }
}
