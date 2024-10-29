/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestRolloverIndexAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestRolloverIndexAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in rollover "
        + "index requests is deprecated. The parameter will be removed in the next major version.";

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_rollover"), new Route(POST, "/{index}/_rollover/{new_index}"));
    }

    @Override
    public String getName() {
        return "rollover_index_action";
    }

    @Override
    public Set<String> supportedCapabilities() {
        if (DataStream.isFailureStoreFeatureFlagEnabled()) {
            return Set.of("lazy-rollover-failure-store");
        } else {
            return Set.of();
        }
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        RolloverRequest rolloverIndexRequest = new RolloverRequest(request.param("index"), request.param("new_index"));
        request.applyContentParser(parser -> rolloverIndexRequest.fromXContent(parser));
        rolloverIndexRequest.dryRun(request.paramAsBoolean("dry_run", false));
        rolloverIndexRequest.lazy(request.paramAsBoolean("lazy", false));
        rolloverIndexRequest.ackTimeout(getAckTimeout(request));
        rolloverIndexRequest.masterNodeTimeout(getMasterNodeTimeout(request));
        if (DataStream.isFailureStoreFeatureFlagEnabled()) {
            boolean failureStore = request.paramAsBoolean("target_failure_store", false);
            if (failureStore) {
                rolloverIndexRequest.setIndicesOptions(
                    IndicesOptions.builder(rolloverIndexRequest.indicesOptions())
                        .selectorOptions(IndicesOptions.SelectorOptions.FAILURES)
                        .build()
                );
            }
        }
        rolloverIndexRequest.getCreateIndexRequest()
            .waitForActiveShards(ActiveShardCount.parseString(request.param("wait_for_active_shards")));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .indices()
            .rolloverIndex(rolloverIndexRequest, new RestToXContentListener<>(channel));
    }

}
