/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class RestGetActionV7 extends RestGetAction {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetActionV7.class);
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in "
        + "document get requests is deprecated, use the /{index}/_doc/{id} endpoint instead.";
    static final String COMPATIBLE_API_MESSAGE = "[Compatible API usage] Index API with types has been removed, use typeless endpoints.";

    @Override
    public String getName() {
        return super.getName() + "_v7";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/{type}/{id}"), new Route(HEAD, "/{index}/{type}/{id}"));
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
        deprecationLogger.deprecate(DeprecationCategory.MAPPINGS, "get_with_types", TYPES_DEPRECATION_MESSAGE);
        deprecationLogger.compatibleApiWarning("get_with_types", COMPATIBLE_API_MESSAGE);
        request.param("type");
        return super.prepareRequest(request, client);
    }
}
