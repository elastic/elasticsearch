/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.integration.server;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.sql.cli.net.protocol.Response;
import org.elasticsearch.xpack.sql.test.server.ProtoHttpServer;

public class CliHttpServer extends ProtoHttpServer<Response> {

    public CliHttpServer(Client client) {
        super(client, new CliProtoHandler(client), "/cli/", "sql/");
    }

    @Override
    public String url() {
        return "http://" + super.url();
    }
}