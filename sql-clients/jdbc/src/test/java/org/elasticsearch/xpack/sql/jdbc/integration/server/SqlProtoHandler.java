/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.server;

import com.sun.net.httpserver.HttpExchange;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.sql.TestUtils;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ProtoUtils;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Request;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Response;
import org.elasticsearch.xpack.sql.plugin.jdbc.server.JdbcServer;
import org.elasticsearch.xpack.sql.plugin.jdbc.server.JdbcServerProtoUtils;
import org.elasticsearch.xpack.sql.test.server.ProtoHandler;

import java.io.DataInput;
import java.io.IOException;

import static org.elasticsearch.action.ActionListener.wrap;

class SqlProtoHandler extends ProtoHandler<Response> {

    private final JdbcServer server;
    
    SqlProtoHandler(Client client) {
        super(client, ProtoUtils::readHeader, JdbcServerProtoUtils::write);
        this.server = new JdbcServer(TestUtils.planExecutor(client), clusterName, () -> info.getNode().getName(), info.getVersion(),
                info.getBuild());
    }

    @Override
    protected void handle(HttpExchange http, DataInput in) throws IOException {
        Request req = ProtoUtils.readRequest(in);
        server.handle(req, wrap(resp -> sendHttpResponse(http, resp), ex -> fail(http, ex)));
    }
}