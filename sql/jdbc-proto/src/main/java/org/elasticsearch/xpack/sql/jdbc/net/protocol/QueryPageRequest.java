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

public class QueryPageRequest extends Request {

    public final String requestId;
    public final TimeoutInfo timeout;

    public QueryPageRequest(String requestId, TimeoutInfo timeout) {
        super(Action.QUERY_PAGE);
        this.requestId = requestId;
        this.timeout = timeout;
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(action.value());
        out.writeUTF(requestId);
        timeout.encode(out);
    }

    public static QueryPageRequest decode(DataInput in) throws IOException {
        String requestId = in.readUTF();
        TimeoutInfo timeout = new TimeoutInfo(in);
        return new QueryPageRequest(requestId, timeout);
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "QueryPageReq[%s]", requestId);
    }
}
