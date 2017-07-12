/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.server.cli;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

public class CliRequestBuilder extends ActionRequestBuilder<CliRequest, CliResponse, CliRequestBuilder> {

    public CliRequestBuilder(ElasticsearchClient client, CliAction action) {
        this(client, action, null);
    }

    public CliRequestBuilder(ElasticsearchClient client, CliAction action, Request req) {
        super(client, action, new CliRequest(req));
    }

    public CliRequestBuilder request(Request req) {
        request.request(req);
        return this;
    }
}
