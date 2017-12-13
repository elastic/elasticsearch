/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryCloseRequest;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryPageRequest;
import org.elasticsearch.xpack.sql.protocol.shared.Nullable;
import org.elasticsearch.xpack.sql.protocol.shared.SqlDataInput;
import org.elasticsearch.xpack.sql.protocol.shared.TimeoutInfo;

import java.io.IOException;

public class QueryCloseRequest extends AbstractQueryCloseRequest {

    public QueryCloseRequest(String cursor) {
        super(cursor);
    }

    QueryCloseRequest(SqlDataInput in) throws IOException {
        super(in);
    }

    @Override
    public RequestType requestType() {
        return RequestType.QUERY_CLOSE;
    }

}
