/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.service;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class WatcherServiceRequestBuilder extends MasterNodeOperationRequestBuilder<WatcherServiceRequest, AcknowledgedResponse,
        WatcherServiceRequestBuilder> {

    public WatcherServiceRequestBuilder(ElasticsearchClient client) {
        super(client, WatcherServiceAction.INSTANCE, new WatcherServiceRequest());
    }

    /**
     * Starts watcher if not already started.
     */
    public WatcherServiceRequestBuilder start() {
        request.start();
        return this;
    }

    /**
     * Stops watcher if not already stopped.
     */
    public WatcherServiceRequestBuilder stop() {
        request.stop();
        return this;
    }
}
