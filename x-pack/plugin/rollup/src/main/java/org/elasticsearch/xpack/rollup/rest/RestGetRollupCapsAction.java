/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.rollup.rest;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.rollup.action.GetRollupCapsAction;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetRollupCapsAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestGetRollupCapsAction.class));

    public static final ParseField ID = new ParseField("id");

    @Override
    public List<Route> routes() {
        return emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return singletonList(new ReplacedRoute(GET, "/_rollup/data/{id}", GET, "/_xpack/rollup/data/{id}/", deprecationLogger));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(ID.getPreferredName());
        GetRollupCapsAction.Request request = new GetRollupCapsAction.Request(id);

        return channel -> client.execute(GetRollupCapsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "get_rollup_caps";
    }

}
