/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class SqlRequestBuilder extends ActionRequestBuilder<SqlRequest, SqlResponse, SqlRequestBuilder> {

    public SqlRequestBuilder(ElasticsearchClient client, SqlAction action) {
        this(client, action, null, null);
    }

    public SqlRequestBuilder(ElasticsearchClient client, SqlAction action, String query, String sessionId) {
        super(client, action, new SqlRequest(query, sessionId));
    }

    public SqlRequestBuilder query(String query) {
        request.query(query);
        return this;
    }

    public SqlRequestBuilder sessionId(String sessionId) {
        request.sessionId(sessionId);
        return this;
    }
}
