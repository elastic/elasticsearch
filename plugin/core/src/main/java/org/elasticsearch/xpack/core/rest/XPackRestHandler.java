/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.XPackClient;

import java.io.IOException;

public abstract class XPackRestHandler extends BaseRestHandler {

    protected static String URI_BASE = "/_xpack";

    public XPackRestHandler(Settings settings) {
        super(settings);
    }

    @Override
    public final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return doPrepareRequest(request, new XPackClient(client));
    }

    protected abstract RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) throws IOException;
}
