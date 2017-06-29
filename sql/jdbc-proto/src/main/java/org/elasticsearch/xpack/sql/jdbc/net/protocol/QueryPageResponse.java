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
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Status;

import static java.lang.String.format;

public class QueryPageResponse extends DataResponse {

    public final String requestId;

    public QueryPageResponse(String requestId, Object data) {
        super(Action.QUERY_PAGE, data);
        this.requestId = requestId;
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(Status.toSuccess(action));
        out.writeUTF(requestId);
    }

    public static QueryPageResponse decode(DataInput in) throws IOException {
        String requestId = in.readUTF();
        return new QueryPageResponse(requestId, null);
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "QueryPageRes[%s]", requestId);
    }
}
