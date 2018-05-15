/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class SqlClearCursorAction
        extends Action<SqlClearCursorRequest, SqlClearCursorResponse, SqlClearCursorRequestBuilder> {

    public static final SqlClearCursorAction INSTANCE = new SqlClearCursorAction();
    public static final String NAME = "indices:data/read/sql/close_cursor";

    private SqlClearCursorAction() {
        super(NAME);
    }

    @Override
    public SqlClearCursorRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new SqlClearCursorRequestBuilder(client, this);
    }

    @Override
    public SqlClearCursorResponse newResponse() {
        return new SqlClearCursorResponse();
    }
}
