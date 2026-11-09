/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
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

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestOpenIndexAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(
        RestUtils.REST_MASTER_TIMEOUT_PARAM,
        RestUtils.REST_TIMEOUT_PARAM,
        IndicesOptions.WildcardOptions.EXPAND_WILDCARDS,
        IndicesOptions.ConcreteTargetOptions.IGNORE_UNAVAILABLE,
        IndicesOptions.WildcardOptions.ALLOW_NO_INDICES,
        IndicesOptions.GatekeeperOptions.IGNORE_THROTTLED,
        "wait_for_active_shards"
    );
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Sets.addToCopy(SUPPORTED_QUERY_PARAMETERS, "index");

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_open"), new Route(POST, "/{index}/_open"));
    }

    @Override
    public String getName() {
        return "open_index_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(Strings.splitStringByCommaToArray(request.param("index")));
        openIndexRequest.ackTimeout(getAckTimeout(request));
        openIndexRequest.masterNodeTimeout(getMasterNodeTimeout(request));
        openIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, openIndexRequest.indicesOptions()));
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            openIndexRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        return channel -> client.admin().indices().open(openIndexRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedQueryParameters() {
        return SUPPORTED_QUERY_PARAMETERS;
    }

    @Override
    public Set<String> allSupportedParameters() {
        return ALL_SUPPORTED_PARAMETERS;
    }

}
