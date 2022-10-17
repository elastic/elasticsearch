/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestIndicesSegmentsAction extends BaseRestHandler {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(RestIndicesSegmentsAction.class);

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
        );
        if (request.hasParam("verbose")) {
            DEPRECATION_LOGGER.warn(
                DeprecationCategory.INDICES,
                "indices_segments_action_verbose",
                "The [verbose] query parameter for [indices_segments_action] has no effect and is deprecated"
            );
        }
        indicesSegmentsRequest.indicesOptions(IndicesOptions.fromRequest(request, indicesSegmentsRequest.indicesOptions()));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .indices()
            .segments(indicesSegmentsRequest, new RestChunkedToXContentListener<>(channel));
    }
}
