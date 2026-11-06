/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.truncate.TruncateIndexAction;
import org.elasticsearch.action.admin.indices.truncate.TruncateIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.Scope.INTERNAL;

// Not exposed in serverless until the API surface has been agreed
@ServerlessScope(INTERNAL)
public class RestTruncateIndexAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_truncate"));
    }

    @Override
    public String getName() {
        return "truncate_index_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final TruncateIndexRequest truncateRequest = new TruncateIndexRequest(Strings.splitStringByCommaToArray(request.param("index")));
        truncateRequest.indicesOptions(IndicesOptions.fromRequest(request, truncateRequest.indicesOptions()));
        return channel -> client.execute(TruncateIndexAction.INSTANCE, truncateRequest, new RestToXContentListener<>(channel));
    }
}
