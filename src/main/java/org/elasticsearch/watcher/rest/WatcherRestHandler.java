/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.rest;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.watcher.client.WatcherClient;

/**
 *
 */
public abstract class WatcherRestHandler extends BaseRestHandler {

    protected static String URI_BASE = "_watcher";

    public WatcherRestHandler(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
    }

    @Override
    protected final void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        handleRequest(request, channel, new WatcherClient(client));
    }

    protected abstract void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception;
}
