/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.createSnapshotRequest;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Creates a new snapshot
 */
public class RestCreateSnapshotAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(PUT, "/_snapshot/{repository}/{snapshot}"),
            new Route(POST, "/_snapshot/{repository}/{snapshot}"));
    }

    @Override
    public String getName() {
        return "create_snapshot_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        CreateSnapshotRequest createSnapshotRequest = createSnapshotRequest(request.param("repository"), request.param("snapshot"));
        request.applyContentParser(p -> createSnapshotRequest.source(p.mapOrdered()));
        createSnapshotRequest.masterNodeTimeout(request.paramAsTime("master_timeout", createSnapshotRequest.masterNodeTimeout()));
        createSnapshotRequest.waitForCompletion(request.paramAsBoolean("wait_for_completion", false));
        return channel -> client.admin().cluster().createSnapshot(createSnapshotRequest, new RestToXContentListener<>(channel) {
            @Override
            public RestResponse buildResponse(CreateSnapshotResponse createSnapshotResponse, XContentBuilder builder) throws Exception {
                final Map<String, String> updatedParams = new HashMap<>(request.params());
                updatedParams.put("include_repository", "false");
                createSnapshotResponse.toXContent(builder, new ToXContent.MapParams(updatedParams));
                return new BytesRestResponse(getStatus(createSnapshotResponse), builder);
            }
        });
    }
}
