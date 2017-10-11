/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.embed;

import com.sun.net.httpserver.HttpExchange;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
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
        action = new RestSqlJdbcAction(Settings.EMPTY, mock(RestController.class), new SqlLicenseChecker(() -> {}, () -> {}));
    }

    @Override
    protected void handle(HttpExchange http, DataInput in) throws IOException {
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        try {
            action.operation(Proto.INSTANCE.readRequest(in), client).accept(channel);
            while (false == channel.await()) {}
            sendHttpResponse(http, channel.capturedResponse().content());
        } catch (Exception e) {
            fail(http, e);
        }
    }
}