/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.document.RestBulkAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class ArrowBulkAction extends BaseRestHandler {

    private final IncrementalBulkService bulkHandler;

    public ArrowBulkAction(Client client, Settings settings) {
        this.bulkHandler = new IncrementalBulkService(client, new IndexingPressure(settings));
    }

    @Override
    public String getName() {
        return "arrow_bulk_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_arrow/_bulk"),
            new Route(PUT, "/_arrow/_bulk"),
            new Route(POST, "/_arrow/{index}/_bulk"),
            new Route(PUT, "/_arrow/{index}/_bulk")
        );
    }

    public boolean mediaTypesValid(RestRequest request) {
        return ArrowBulkRequestParser.isArrowRequest(request);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String waitForActiveShards = request.param("wait_for_active_shards");
        TimeValue timeout = request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT);
        String refresh = request.param("refresh");
        return new RestBulkAction.ChunkHandler(
            false,
            request,
            () -> bulkHandler.newBulkRequest(waitForActiveShards, timeout, refresh),
            new ArrowBulkRequestParser(request)
        );
    }
}
