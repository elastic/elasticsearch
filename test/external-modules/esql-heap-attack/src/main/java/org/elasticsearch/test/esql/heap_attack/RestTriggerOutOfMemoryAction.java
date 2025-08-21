/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.esql.heap_attack;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestTriggerOutOfMemoryAction extends BaseRestHandler {
    private static final Logger LOGGER = LogManager.getLogger(RestTriggerOutOfMemoryAction.class);

    @Override
    public String getName() {
        return "trigger_out_of_memory";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_trigger_out_of_memory"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        LOGGER.error("triggering out of memory");
        List<int[]> values = new ArrayList<>();
        return channel -> {
            while (true) {
                values.add(new int[1024 * 1024]);
            }
        };
    }
}
