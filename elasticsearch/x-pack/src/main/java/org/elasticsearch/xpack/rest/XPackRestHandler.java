/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rest;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.XPackClient;

/**
 *
 */
public abstract class XPackRestHandler extends BaseRestHandler {

    protected static String URI_BASE = "_xpack";

    public XPackRestHandler(Settings settings, Client client) {
        super(settings, client);
    }

    @Override
    protected final void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        handleRequest(request, channel, new XPackClient(client));
    }

    protected abstract void handleRequest(RestRequest request, RestChannel channel, XPackClient client) throws Exception;
}
