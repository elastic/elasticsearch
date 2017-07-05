/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Response;
import org.elasticsearch.xpack.sql.test.server.ProtoHttpServer;

/**
 * Internal server used for testing without starting a new Elasticsearch instance.
 */
public class JdbcHttpServer extends ProtoHttpServer<Response> {

    public JdbcHttpServer(Client client) {
        super(client, new SqlProtoHandler(client), "/_jdbc");
    }

    @Override
    public String url() {
        return "jdbc:es://" + super.url();
    }
}