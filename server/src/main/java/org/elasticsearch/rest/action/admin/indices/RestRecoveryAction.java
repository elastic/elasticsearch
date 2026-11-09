/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.support.IndicesOptions;
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

/**
 * REST handler to report on index recoveries.
 */
@ServerlessScope(Scope.INTERNAL)
public class RestRecoveryAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(
        IndicesOptions.WildcardOptions.EXPAND_WILDCARDS,
        IndicesOptions.ConcreteTargetOptions.IGNORE_UNAVAILABLE,
        IndicesOptions.WildcardOptions.ALLOW_NO_INDICES,
        IndicesOptions.GatekeeperOptions.IGNORE_THROTTLED,
        "detailed",
        "active_only"
    );
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Sets.addToCopy(SUPPORTED_QUERY_PARAMETERS, "index");

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_recovery"), new Route(GET, "/{index}/_recovery"));
    }

    @Override
    public String getName() {
        return "recovery_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {

        final RecoveryRequest recoveryRequest = new RecoveryRequest(Strings.splitStringByCommaToArray(request.param("index")));
        recoveryRequest.detailed(request.paramAsBoolean("detailed", false));
        recoveryRequest.activeOnly(request.paramAsBoolean("active_only", false));
        recoveryRequest.indicesOptions(IndicesOptions.fromRequest(request, recoveryRequest.indicesOptions()));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .indices()
            .recoveries(recoveryRequest, new RestRefCountedChunkedToXContentListener<>(channel));
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
