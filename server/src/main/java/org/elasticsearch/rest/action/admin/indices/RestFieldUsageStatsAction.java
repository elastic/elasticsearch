/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.stats.FieldUsageStatsAction;
import org.elasticsearch.action.admin.indices.stats.FieldUsageStatsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class RestFieldUsageStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new RestHandler.Route(RestRequest.Method.GET, "/{index}/_field_usage_stats"));
    }

    @Override
    public String getName() {
        return "field_usage_stats_action";
    }

    @Override
    public BaseRestHandler.RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, IndicesOptions.strictExpandOpenAndForbidClosed());
        final FieldUsageStatsRequest fusRequest = new FieldUsageStatsRequest(indices, indicesOptions);
        fusRequest.fields(request.paramAsStringArray("fields", fusRequest.fields()));
        return channel -> {
            final RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(FieldUsageStatsAction.INSTANCE, fusRequest, new RestToXContentListener<>(channel));
        };
    }
}
