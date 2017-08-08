/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.jdbc.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

import java.io.IOException;

public class JdbcRequestBuilder extends ActionRequestBuilder<JdbcRequest, JdbcResponse, JdbcRequestBuilder> {

    public JdbcRequestBuilder(ElasticsearchClient client, JdbcAction action) {
        super(client, action, new JdbcRequest());
    }

    public JdbcRequestBuilder(ElasticsearchClient client, JdbcAction action, Request req) {
        super(client, action, new JdbcRequest(req));
    }

    public JdbcRequestBuilder request(Request req) throws IOException {
        request.request(req);
        return this;
    }
}
