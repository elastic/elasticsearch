/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class SqlAction extends Action<SqlRequest, SqlResponse, SqlRequestBuilder> {

    public static final SqlAction INSTANCE = new SqlAction();
    public static final String NAME = "indices:data/read/sql";

    private SqlAction() {
        super(NAME);
    }

    @Override
    public SqlRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new SqlRequestBuilder(client, this);
    }

    @Override
    public SqlResponse newResponse() {
        return new SqlResponse();
    }
}
