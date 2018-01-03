/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class SqlListColumnsAction extends Action<SqlListColumnsRequest, SqlListColumnsResponse, SqlListColumnsRequestBuilder> {

    public static final SqlListColumnsAction INSTANCE = new SqlListColumnsAction();
    public static final String NAME = "indices:admin/sql/columns";
    public static final String REST_ENDPOINT = "/_xpack/sql/columns";

    private SqlListColumnsAction() {
        super(NAME);
    }

    @Override
    public SqlListColumnsRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new SqlListColumnsRequestBuilder(client, this);
    }

    @Override
    public SqlListColumnsResponse newResponse() {
        return new SqlListColumnsResponse();
    }
}
