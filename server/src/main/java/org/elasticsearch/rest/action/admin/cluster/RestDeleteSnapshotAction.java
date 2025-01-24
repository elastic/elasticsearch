/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

/**
 * Deletes a snapshot
 */
@ServerlessScope(Scope.INTERNAL)
public class RestDeleteSnapshotAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(RestUtils.REST_MASTER_TIMEOUT_PARAM, "wait_for_completion");
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Set.copyOf(
        Sets.union(SUPPORTED_QUERY_PARAMETERS, Set.of("repository", "snapshot"))
    );

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_snapshot/{repository}/{snapshot}"));
    }

    @Override
    public String getName() {
        return "delete_snapshot_action";
    }

    @Override
    public Set<String> allSupportedParameters() {
        return ALL_SUPPORTED_PARAMETERS;
    }

    @Override
    public Set<String> supportedQueryParameters() {
        return SUPPORTED_QUERY_PARAMETERS;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String repository = request.param("repository");
        String[] snapshots = Strings.splitStringByCommaToArray(request.param("snapshot"));
        final var deleteSnapshotRequest = new DeleteSnapshotRequest(getMasterNodeTimeout(request), repository, snapshots);
        deleteSnapshotRequest.waitForCompletion(request.paramAsBoolean("wait_for_completion", deleteSnapshotRequest.waitForCompletion()));
        return channel -> client.admin().cluster().deleteSnapshot(deleteSnapshotRequest, new RestToXContentListener<>(channel));
    }
}
