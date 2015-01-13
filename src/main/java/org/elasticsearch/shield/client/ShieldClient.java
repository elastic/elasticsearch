/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;

/**
 * A wrapper to elasticsearch clients that exposes all Shield related APIs
 */
public class ShieldClient {

    private final ShieldAuthcClient authcClient;

    public ShieldClient(Client client) {
        this(client.admin().cluster());
    }

    public ShieldClient(ClusterAdminClient client) {
        this.authcClient = new ShieldAuthcClient(client);
    }

    /**
     * @return  The Shield authenticatin client.
     */
    public ShieldAuthcClient authc() {
        return authcClient;
    }

}
