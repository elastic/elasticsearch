/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

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
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final boolean includeTypeName = includeTypeName(request);
        RolloverRequest rolloverIndexRequest = new RolloverRequest(request.param("index"), request.param("new_index"));
        request.applyContentParser(parser -> rolloverIndexRequest.fromXContent(includeTypeName, parser));
        rolloverIndexRequest.dryRun(request.paramAsBoolean("dry_run", false));
        rolloverIndexRequest.timeout(request.paramAsTime("timeout", rolloverIndexRequest.timeout()));
        rolloverIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", rolloverIndexRequest.masterNodeTimeout()));
        rolloverIndexRequest.getCreateIndexRequest()
            .waitForActiveShards(ActiveShardCount.parseString(request.param("wait_for_active_shards")));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .indices()
            .rolloverIndex(rolloverIndexRequest, new RestToXContentListener<>(channel));
    }

    private static boolean includeTypeName(RestRequest request) {
        boolean includeTypeName = false;
        if (request.getRestApiVersion() == RestApiVersion.V_7) {
            if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
                deprecationLogger.compatibleCritical("index_rollover_with_types", TYPES_DEPRECATION_MESSAGE);
            }
            includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);
        }
        return includeTypeName;
    }
}
