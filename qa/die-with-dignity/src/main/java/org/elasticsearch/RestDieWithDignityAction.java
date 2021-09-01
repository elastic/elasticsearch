/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestDieWithDignityAction extends BaseRestHandler {

    RestDieWithDignityAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_die_with_dignity"));
    }

    @Override
    public String getName() {
        return "die_with_dignity_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        throw new OutOfMemoryError("die with dignity");
    }

}
