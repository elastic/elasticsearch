/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.stats.FieldUsageStatsAction;
import org.elasticsearch.action.admin.indices.stats.FieldUsageStatsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@ServerlessScope(value = Scope.INTERNAL)
public class RestFieldUsageStatsAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(
        IndicesOptions.WildcardOptions.EXPAND_WILDCARDS,
        IndicesOptions.ConcreteTargetOptions.IGNORE_UNAVAILABLE,
        IndicesOptions.WildcardOptions.ALLOW_NO_INDICES,
        IndicesOptions.GatekeeperOptions.IGNORE_THROTTLED,
        "fields"
    );
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Sets.addToCopy(SUPPORTED_QUERY_PARAMETERS, "index");

    private static final String DOC_VALUES_SKIPPER_CAPABILITY = "doc_values_skipper";

    @Override
    public List<Route> routes() {
        return List.of(new RestHandler.Route(RestRequest.Method.GET, "/{index}/_field_usage_stats"));
    }

    @Override
    public String getName() {
        return "field_usage_stats_action";
    }

    @Override
    public BaseRestHandler.RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, IndicesOptions.strictExpandOpenAndForbidClosed());
        final FieldUsageStatsRequest fusRequest = new FieldUsageStatsRequest(indices, indicesOptions);
        fusRequest.fields(request.paramAsStringArray("fields", fusRequest.fields()));
        return channel -> {
            final RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(FieldUsageStatsAction.INSTANCE, fusRequest, new RestRefCountedChunkedToXContentListener<>(channel));
        };
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(DOC_VALUES_SKIPPER_CAPABILITY);
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
