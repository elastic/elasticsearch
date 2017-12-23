/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.embed;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto;
import org.elasticsearch.xpack.sql.plugin.RestSqlJdbcAction;
import org.elasticsearch.xpack.sql.plugin.SqlLicenseChecker;

import java.io.DataInput;
import java.io.IOException;

import static org.mockito.Mockito.mock;

class JdbcProtoHandler extends ProtoHandler {
    private final RestSqlJdbcAction action;

    JdbcProtoHandler(Client client) {
        super(client);
        action = new RestSqlJdbcAction(Settings.EMPTY, mock(RestController.class), new SqlLicenseChecker(() -> {}, () -> {}),
                new IndexResolver(client));
    }

    @Override
    protected void handle(RestChannel channel, DataInput in) throws IOException {
        action.operation(Proto.INSTANCE.readRequest(in), client).accept(channel);
    }
}