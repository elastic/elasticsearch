/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.service;

import org.elasticsearch.watcher.client.WatcherAction;
import org.elasticsearch.client.Client;

/**
 */
public class WatcherServiceAction extends WatcherAction<WatcherServiceRequest, WatcherServiceResponse, WatcherServiceRequestBuilder> {

    public static final WatcherServiceAction INSTANCE = new WatcherServiceAction();
    public static final String NAME = "cluster:admin/watcher/service";

    private WatcherServiceAction() {
        super(NAME);
    }

    @Override
    public WatcherServiceResponse newResponse() {
        return new WatcherServiceResponse();
    }

    @Override
    public WatcherServiceRequestBuilder newRequestBuilder(Client client) {
        return new WatcherServiceRequestBuilder(client);
    }

}
