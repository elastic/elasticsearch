/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.server.cli;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class CliAction extends Action<CliRequest, CliResponse, CliRequestBuilder> {

    public static final CliAction INSTANCE = new CliAction();
    public static final String NAME = "indices:data/read/sql/cli";

    private CliAction() {
        super(NAME);
    }

    @Override
    public CliRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new CliRequestBuilder(client, this);
    }

    @Override
    public CliResponse newResponse() {
        return new CliResponse();
    }
}
