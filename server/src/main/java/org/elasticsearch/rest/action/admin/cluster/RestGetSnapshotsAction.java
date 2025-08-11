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
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.DispatchingRestToXContentListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.elasticsearch.client.Requests.getSnapshotsRequest;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.snapshots.SnapshotInfo.INCLUDE_REPOSITORY_XCONTENT_PARAM;
import static org.elasticsearch.snapshots.SnapshotInfo.INDEX_DETAILS_XCONTENT_PARAM;

/**
 * Returns information about snapshot
 */
public class RestGetSnapshotsAction extends BaseRestHandler {

    private final ThreadPool threadPool;

    public RestGetSnapshotsAction(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_snapshot/{repository}/{snapshot}"));
    }

    @Override
    public String getName() {
        return "get_snapshots_action";
    }

    @Override
    protected Set<String> responseParams() {
        return org.elasticsearch.core.Set.of(INDEX_DETAILS_XCONTENT_PARAM, INCLUDE_REPOSITORY_XCONTENT_PARAM);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] repositories = request.paramAsStringArray("repository", Strings.EMPTY_ARRAY);
        String[] snapshots = request.paramAsStringArray("snapshot", Strings.EMPTY_ARRAY);

        GetSnapshotsRequest getSnapshotsRequest = getSnapshotsRequest(repositories).snapshots(snapshots);
        getSnapshotsRequest.ignoreUnavailable(request.paramAsBoolean("ignore_unavailable", getSnapshotsRequest.ignoreUnavailable()));
        getSnapshotsRequest.verbose(request.paramAsBoolean("verbose", getSnapshotsRequest.verbose()));
        final GetSnapshotsRequest.SortBy sort = GetSnapshotsRequest.SortBy.of(request.param("sort", getSnapshotsRequest.sort().toString()));
        getSnapshotsRequest.sort(sort);
        final int size = request.paramAsInt("size", getSnapshotsRequest.size());
        getSnapshotsRequest.size(size);
        final int offset = request.paramAsInt("offset", getSnapshotsRequest.offset());
        getSnapshotsRequest.offset(offset);
        final String afterString = request.param("after");
        if (afterString != null) {
            getSnapshotsRequest.after(GetSnapshotsRequest.After.fromQueryParam(afterString));
        }
        final String fromSortValue = request.param("from_sort_value");
        if (fromSortValue != null) {
            getSnapshotsRequest.fromSortValue(fromSortValue);
        }
        final String[] policies = request.paramAsStringArray("slm_policy_filter", Strings.EMPTY_ARRAY);
        getSnapshotsRequest.policies(policies);

        final SortOrder order = SortOrder.fromString(request.param("order", getSnapshotsRequest.order().toString()));
        getSnapshotsRequest.order(order);
        getSnapshotsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getSnapshotsRequest.masterNodeTimeout()));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .cluster()
            .getSnapshots(
                getSnapshotsRequest,
                new DispatchingRestToXContentListener<>(threadPool.executor(ThreadPool.Names.MANAGEMENT), channel, request).map(
                    r -> StatusToXContentObject.withStatus(RestStatus.OK, r)
                )
            );
    }
}
