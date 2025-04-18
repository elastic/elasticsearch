/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestRolloverIndexAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_rollover"), new Route(POST, "/{index}/_rollover/{new_index}"));
    }

    @Override
    public String getName() {
        return "rollover_index_action";
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of("return-404-on-missing-target", "index_expression_selectors");
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        RolloverRequest rolloverIndexRequest = new RolloverRequest(request.param("index"), request.param("new_index"));
        request.applyContentParser(parser -> rolloverIndexRequest.fromXContent(parser));
        rolloverIndexRequest.dryRun(request.paramAsBoolean("dry_run", false));
        rolloverIndexRequest.lazy(request.paramAsBoolean("lazy", false));
        rolloverIndexRequest.ackTimeout(getAckTimeout(request));
        rolloverIndexRequest.masterNodeTimeout(getMasterNodeTimeout(request));
        rolloverIndexRequest.getCreateIndexRequest()
            .waitForActiveShards(ActiveShardCount.parseString(request.param("wait_for_active_shards")));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .indices()
            .rolloverIndex(rolloverIndexRequest, new RestToXContentListener<>(channel));
    }

}
