/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.server.jdbc;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class JdbcAction extends Action<JdbcRequest, JdbcResponse, JdbcRequestBuilder> {

    public static final JdbcAction INSTANCE = new JdbcAction();
    public static final String NAME = "indices:data/read/sql/jdbc";

    private JdbcAction() {
        super(NAME);
    }

    @Override
    public JdbcRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new JdbcRequestBuilder(client, this);
    }

    @Override
    public JdbcResponse newResponse() {
        return new JdbcResponse();
    }
}
