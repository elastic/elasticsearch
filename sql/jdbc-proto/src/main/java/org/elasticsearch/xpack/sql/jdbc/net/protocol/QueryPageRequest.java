/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryPageRequest;
import org.elasticsearch.xpack.sql.protocol.shared.Nullable;
import org.elasticsearch.xpack.sql.protocol.shared.TimeoutInfo;
import org.elasticsearch.xpack.sql.protocol.shared.SqlDataInput;

import java.io.IOException;

public class QueryPageRequest extends AbstractQueryPageRequest {
    private final transient Payload data;

    public QueryPageRequest(byte[] cursor, TimeoutInfo timeout, @Nullable Payload data) {
        super(cursor, timeout);
        this.data = data;
    }

    QueryPageRequest(SqlDataInput in) throws IOException {
        super(in);
        this.data = null; // data isn't used on the server side
    }

    public Payload data() {
        return data;
    }

    @Override
    public RequestType requestType() {
        return RequestType.QUERY_PAGE;
    }

    // not overriding hashCode and equals because we're intentionally ignore the data field
}
