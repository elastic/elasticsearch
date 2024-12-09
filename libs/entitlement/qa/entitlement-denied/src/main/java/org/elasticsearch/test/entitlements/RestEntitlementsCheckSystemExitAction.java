/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.entitlements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestEntitlementsCheckSystemExitAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestEntitlementsCheckSystemExitAction.class);

    public RestEntitlementsCheckSystemExitAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_entitlement/negative/_check_system_exit"));
    }

    @Override
    public String getName() {
        return "check_system_exit_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        logger.info("RestEntitlementsCheckSystemExitAction rest handler");
        return channel -> {
            logger.info("Calling System.exit(123);");
            System.exit(123);
        };
    }
}
