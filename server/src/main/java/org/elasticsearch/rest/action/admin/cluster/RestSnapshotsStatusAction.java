/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_PARAM;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

/**
 * Returns status of currently running snapshot
 */
@ServerlessScope(Scope.INTERNAL)
public class RestSnapshotsStatusAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(REST_MASTER_TIMEOUT_PARAM, "ignore_unavailable");
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Sets.addToCopy(SUPPORTED_QUERY_PARAMETERS, "repository", "snapshot");

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_snapshot/{repository}/{snapshot}/_status"),
            new Route(GET, "/_snapshot/{repository}/_status"),
            new Route(GET, "/_snapshot/_status")
        );
    }

    @Override
    public String getName() {
        return "snapshot_status_action";
    }

    @Override
    public Set<String> supportedQueryParameters() {
        return SUPPORTED_QUERY_PARAMETERS;
    }

    @Override
    public Set<String> allSupportedParameters() {
        return ALL_SUPPORTED_PARAMETERS;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] snapshots = request.paramAsStringArray("snapshot", Strings.EMPTY_ARRAY);
        if (snapshots.length == 1 && "_all".equalsIgnoreCase(snapshots[0])) {
            snapshots = Strings.EMPTY_ARRAY;
        }
        final var snapshotsStatusRequest = new SnapshotsStatusRequest(getMasterNodeTimeout(request), request.param("repository", "_all"));
        snapshotsStatusRequest.snapshots(snapshots);
        snapshotsStatusRequest.ignoreUnavailable(request.paramAsBoolean("ignore_unavailable", snapshotsStatusRequest.ignoreUnavailable()));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .cluster()
            .snapshotsStatus(snapshotsStatusRequest, new RestRefCountedChunkedToXContentListener<>(channel));
    }
}
