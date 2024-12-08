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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestEntitlementsCheckClassLoaderAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestEntitlementsCheckClassLoaderAction.class);

    RestEntitlementsCheckClassLoaderAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_entitlement/_check_create_url_classloader"));
    }

    @Override
    public String getName() {
        return "check_classloader_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        logger.info("RestEntitlementsCheckClassLoaderAction rest handler [{}]", request.path());
        if (request.path().equals("/_entitlement/_check_create_url_classloader")) {
            return channel -> {
                logger.info("Calling new URLClassLoader");
                try (var classLoader = new URLClassLoader("test", new URL[0], this.getClass().getClassLoader())) {
                    logger.info("Created URLClassLoader [{}]", classLoader.getName());
                }
            };
        }

        throw new UnsupportedOperationException();
    }
}
