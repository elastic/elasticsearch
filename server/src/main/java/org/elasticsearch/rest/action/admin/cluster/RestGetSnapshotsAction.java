/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.client.Requests.getSnapshotsRequest;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.snapshots.SnapshotInfo.INDEX_DETAILS_XCONTENT_PARAM;

/**
 * Returns information about snapshot
 */
public class RestGetSnapshotsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_snapshot/{repository}/{snapshot}"));
    }

    @Override
    public String getName() {
        return "get_snapshots_action";
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(INDEX_DETAILS_XCONTENT_PARAM);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] repositories = request.paramAsStringArray("repository", Strings.EMPTY_ARRAY);
        String[] snapshots = request.paramAsStringArray("snapshot", Strings.EMPTY_ARRAY);

        GetSnapshotsRequest getSnapshotsRequest = getSnapshotsRequest(repositories).snapshots(snapshots);
        getSnapshotsRequest.ignoreUnavailable(request.paramAsBoolean("ignore_unavailable", getSnapshotsRequest.ignoreUnavailable()));
        getSnapshotsRequest.verbose(request.paramAsBoolean("verbose", getSnapshotsRequest.verbose()));
        getSnapshotsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getSnapshotsRequest.masterNodeTimeout()));
        return channel -> client.admin().cluster().getSnapshots(getSnapshotsRequest, new RestToXContentListener<>(channel));
    }
}
