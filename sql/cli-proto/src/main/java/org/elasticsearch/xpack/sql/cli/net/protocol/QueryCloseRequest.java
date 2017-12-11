/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryCloseRequest;
import org.elasticsearch.xpack.sql.protocol.shared.SqlDataInput;

import java.io.IOException;

public class QueryCloseRequest extends AbstractQueryCloseRequest {
    public QueryCloseRequest(String cursor) {
        super(cursor);
    }

    QueryCloseRequest(SqlDataInput in) throws IOException {
        super(in);
    }

    @Override
    public Proto.RequestType requestType() {
        return Proto.RequestType.QUERY_CLOSE;
    }
}
