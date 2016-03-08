/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;

public class MonitoringClient {

    private final Client client;

    @Inject
    public MonitoringClient(Client client) {
        this.client = client;
    }

    // to be implemented: specific API for monitoring
}
