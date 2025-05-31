/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.SnapshotSortKey;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotState;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;
import static org.elasticsearch.snapshots.SnapshotInfo.INCLUDE_REPOSITORY_XCONTENT_PARAM;
import static org.elasticsearch.snapshots.SnapshotInfo.INDEX_DETAILS_XCONTENT_PARAM;
import static org.elasticsearch.snapshots.SnapshotInfo.INDEX_NAMES_XCONTENT_PARAM;

/**
 * Returns information about snapshot
 */
@ServerlessScope(Scope.INTERNAL)
public class RestGetSnapshotsAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_RESPONSE_PARAMETERS = Set.of(
        INCLUDE_REPOSITORY_XCONTENT_PARAM,
        INDEX_DETAILS_XCONTENT_PARAM,
        INDEX_NAMES_XCONTENT_PARAM
    );

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(
        RestUtils.REST_MASTER_TIMEOUT_PARAM,
        "after",
        "from_sort_value",
        "ignore_unavailable",
        "offset",
        "order",
        "size",
        "slm_policy_filter",
        "sort",
        "state",
        "verbose"
    );

    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Set.copyOf(
        Sets.union(SUPPORTED_QUERY_PARAMETERS, SUPPORTED_RESPONSE_PARAMETERS, Set.of("repository", "snapshot"))
    );

    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestGetSnapshotsAction(Predicate<NodeFeature> clusterSupportsFeature) {
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

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
        return SUPPORTED_RESPONSE_PARAMETERS;
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
        String[] repositories = request.paramAsStringArray("repository", Strings.EMPTY_ARRAY);
        String[] snapshots = request.paramAsStringArray("snapshot", Strings.EMPTY_ARRAY);

        final var getSnapshotsRequest = new GetSnapshotsRequest(getMasterNodeTimeout(request), repositories).snapshots(snapshots);
        getSnapshotsRequest.ignoreUnavailable(request.paramAsBoolean("ignore_unavailable", getSnapshotsRequest.ignoreUnavailable()));
        getSnapshotsRequest.verbose(request.paramAsBoolean("verbose", getSnapshotsRequest.verbose()));
        final SnapshotSortKey sort = SnapshotSortKey.of(request.param("sort", getSnapshotsRequest.sort().toString()));
        getSnapshotsRequest.sort(sort);
        final int size = request.paramAsInt("size", getSnapshotsRequest.size());
        getSnapshotsRequest.size(size);
        final int offset = request.paramAsInt("offset", getSnapshotsRequest.offset());
        getSnapshotsRequest.offset(offset);
        final String afterString = request.param("after");
        if (afterString != null) {
            getSnapshotsRequest.after(SnapshotSortKey.decodeAfterQueryParam(afterString));
        }
        final String fromSortValue = request.param("from_sort_value");
        if (fromSortValue != null) {
            getSnapshotsRequest.fromSortValue(fromSortValue);
        }
        final String[] policies = request.paramAsStringArray("slm_policy_filter", Strings.EMPTY_ARRAY);
        getSnapshotsRequest.policies(policies);

        final SortOrder order = SortOrder.fromString(request.param("order", getSnapshotsRequest.order().toString()));
        getSnapshotsRequest.order(order);
        getSnapshotsRequest.includeIndexNames(request.paramAsBoolean(INDEX_NAMES_XCONTENT_PARAM, getSnapshotsRequest.includeIndexNames()));

        final String stateString = request.param("state");
        if (Strings.hasLength(stateString) == false) {
            getSnapshotsRequest.states(EnumSet.allOf(SnapshotState.class));
        } else if (clusterSupportsFeature.test(GetSnapshotsFeatures.GET_SNAPSHOTS_STATE_PARAMETER)) {
            getSnapshotsRequest.states(EnumSet.copyOf(Arrays.stream(stateString.split(",")).map(SnapshotState::valueOf).toList()));
        } else {
            throw new IllegalStateException("[state] parameter is not supported on all nodes in the cluster");
        }

        // Consume these response parameters used in SnapshotInfo now, to avoid assertion errors in BaseRestHandler for requests where they
        // may not get used.
        if (Assertions.ENABLED) {
            for (final var responseParameter : SUPPORTED_RESPONSE_PARAMETERS) {
                request.param(responseParameter);
            }
        }

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .cluster()
            .getSnapshots(getSnapshotsRequest, new RestRefCountedChunkedToXContentListener<>(channel));
    }
}
