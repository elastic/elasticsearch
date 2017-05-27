/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.jdbc.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Request;

public class JdbcRequestBuilder extends ActionRequestBuilder<JdbcRequest, JdbcResponse, JdbcRequestBuilder> {

    public JdbcRequestBuilder(ElasticsearchClient client, JdbcAction action) {
        this(client, action, null);
    }

    public JdbcRequestBuilder(ElasticsearchClient client, JdbcAction action, Request req) {
        super(client, action, new JdbcRequest(req));
    }

    public JdbcRequestBuilder request(Request req) {
        request.request(req);
        return this;
    }
}
