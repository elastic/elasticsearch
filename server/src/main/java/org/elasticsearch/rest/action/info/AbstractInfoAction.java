/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.info;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

public abstract class AbstractInfoAction extends BaseRestHandler {

    public abstract NodesStatsRequest buildNodeStatsRequest();

    public abstract ChunkedToXContent xContentChunks(NodesStatsResponse nodesStatsResponse);

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> client.admin().cluster().nodesStats(buildNodeStatsRequest(), new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(NodesStatsResponse nodesStatsResponse) throws Exception {
                return new RestResponse(
                    RestStatus.OK,
                    ChunkedRestResponseBody.fromXContent(
                        outerParams -> Iterators.concat(
                            ChunkedToXContentHelper.startObject(),
                            Iterators.single(
                                (builder, params) -> builder.field("cluster_name", nodesStatsResponse.getClusterName().value())
                            ),
                            xContentChunks(nodesStatsResponse).toXContentChunked(outerParams),
                            ChunkedToXContentHelper.endObject()
                        ),
                        EMPTY_PARAMS,
                        channel
                    )
                );
            }
        });
    }
}
