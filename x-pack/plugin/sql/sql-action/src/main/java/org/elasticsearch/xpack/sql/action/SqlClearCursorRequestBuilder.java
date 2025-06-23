/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class SqlClearCursorRequestBuilder extends ActionRequestBuilder<SqlClearCursorRequest, SqlClearCursorResponse> {

    public SqlClearCursorRequestBuilder(ElasticsearchClient client) {
        super(client, SqlClearCursorAction.INSTANCE, new SqlClearCursorRequest());
    }

    public SqlClearCursorRequestBuilder cursor(String cursor) {
        request.setCursor(cursor);
        return this;
    }
}
