/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryPageRequest;
import org.elasticsearch.xpack.sql.protocol.shared.TimeoutInfo;
import org.elasticsearch.xpack.sql.protocol.shared.SqlDataInput;

import java.io.IOException;

public class QueryPageRequest extends AbstractQueryPageRequest {
    public QueryPageRequest(byte[] cursor, TimeoutInfo timeout) {
        super(cursor, timeout);
    }

    QueryPageRequest(SqlDataInput in) throws IOException {
        super(in);
    }

    @Override
    public RequestType requestType() {
        return RequestType.QUERY_PAGE;
    }
}
