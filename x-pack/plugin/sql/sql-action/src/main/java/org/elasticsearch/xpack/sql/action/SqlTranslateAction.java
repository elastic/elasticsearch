/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Sql action for translating SQL queries into ES requests
 */
public class SqlTranslateAction extends Action<SqlTranslateRequest, SqlTranslateResponse, SqlTranslateRequestBuilder> {

    public static final SqlTranslateAction INSTANCE = new SqlTranslateAction();
    public static final String NAME = "indices:data/read/sql/translate";

    private SqlTranslateAction() {
        super(NAME);
    }

    @Override
    public SqlTranslateRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new SqlTranslateRequestBuilder(client, this);
    }

    @Override
    public SqlTranslateResponse newResponse() {
        return new SqlTranslateResponse();
    }
}
