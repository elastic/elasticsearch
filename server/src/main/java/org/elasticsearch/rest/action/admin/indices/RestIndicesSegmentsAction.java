/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public class RestIndicesSegmentsAction extends BaseRestHandler {

    public RestIndicesSegmentsAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_segments"), new Route(GET, "/{index}/_segments"));
    }

    @Override
    public String getName() {
        return "indices_segments_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        IndicesSegmentsRequest indicesSegmentsRequest = new IndicesSegmentsRequest(
            Strings.splitStringByCommaToArray(request.param("index"))
        ).withVectorFormatsInfo(request.paramAsBoolean("vector_formats", false));
        indicesSegmentsRequest.indicesOptions(IndicesOptions.fromRequest(request, indicesSegmentsRequest.indicesOptions()));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .indices()
            .segments(indicesSegmentsRequest, new RestRefCountedChunkedToXContentListener<>(channel));
    }
}
