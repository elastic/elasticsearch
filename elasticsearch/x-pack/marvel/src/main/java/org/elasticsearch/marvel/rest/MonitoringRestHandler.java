/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.rest;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.client.MonitoringClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.Locale;

public abstract class MonitoringRestHandler extends BaseRestHandler {

    protected static String URI_BASE = String.format(Locale.ROOT, "/_%s/monitoring", XPackPlugin.NAME);

    public MonitoringRestHandler(Settings settings, Client client) {
        super(settings, client);
    }

    @Override
    protected final void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        handleRequest(request, channel, new MonitoringClient(client));
    }

    protected abstract void handleRequest(RestRequest request, RestChannel channel, MonitoringClient client) throws Exception;
}
