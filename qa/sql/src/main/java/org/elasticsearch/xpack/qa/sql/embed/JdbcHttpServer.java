/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.embed;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.sql.protocol.shared.Response;

/**
 * Internal server used for testing without starting a new Elasticsearch instance.
 */
public class JdbcHttpServer extends ProtoHttpServer<Response> {

    public JdbcHttpServer(Client client) {
        super(client, new JdbcProtoHandler(client), "/_jdbc");
    }

    @Override
    public String url() {
        return "jdbc:es://" + super.url();
    }
}