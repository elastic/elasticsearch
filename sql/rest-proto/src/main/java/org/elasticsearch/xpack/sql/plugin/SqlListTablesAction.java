/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class SqlListTablesAction extends Action<SqlListTablesRequest, SqlListTablesResponse, SqlListTablesRequestBuilder> {

    public static final SqlListTablesAction INSTANCE = new SqlListTablesAction();
    public static final String NAME = "indices:admin/sql/tables";
    public static final String REST_ENDPOINT = "/_xpack/sql/tables";

    private SqlListTablesAction() {
        super(NAME);
    }

    @Override
    public SqlListTablesRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new SqlListTablesRequestBuilder(client, this);
    }

    @Override
    public SqlListTablesResponse newResponse() {
        return new SqlListTablesResponse();
    }
}
