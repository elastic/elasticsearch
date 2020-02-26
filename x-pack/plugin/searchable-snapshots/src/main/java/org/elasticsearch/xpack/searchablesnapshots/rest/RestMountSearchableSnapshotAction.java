/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.searchablesnapshots.action.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.searchablesnapshots.action.MountSearchableSnapshotRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestMountSearchableSnapshotAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "mount_snapshot_action";
    }

    @Override
    public List<Route> routes() {
        return Arrays.asList(new Route(POST, "/{index}/_snapshot/mount"),
            new Route(PUT, "/{index}/_snapshot/mount"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        MountSearchableSnapshotRequest restoreRequest = new MountSearchableSnapshotRequest(request.param("index"));
        restoreRequest.masterNodeTimeout(request.paramAsTime("master_timeout", restoreRequest.masterNodeTimeout()));
        restoreRequest.waitForCompletion(request.paramAsBoolean("wait_for_completion", false));
        request.applyContentParser(p -> restoreRequest.source(p.mapOrdered()));
        return channel -> client.execute(MountSearchableSnapshotAction.INSTANCE, restoreRequest, new RestToXContentListener<>(channel));
    }
}
