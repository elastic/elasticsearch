/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class SqlListTablesRequestBuilder extends
        ActionRequestBuilder<SqlListTablesRequest, SqlListTablesResponse, SqlListTablesRequestBuilder> {

    public SqlListTablesRequestBuilder(ElasticsearchClient client, SqlListTablesAction action) {
        super(client, action, new SqlListTablesRequest());
    }

    public SqlListTablesRequestBuilder pattern(String pattern) {
        request.setPattern(pattern);
        return this;
    }
}
