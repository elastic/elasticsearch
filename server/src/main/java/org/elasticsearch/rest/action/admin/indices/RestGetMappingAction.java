/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestGetMappingAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(
        RestUtils.REST_MASTER_TIMEOUT_PARAM,
        "local",
        IndicesOptions.WildcardOptions.EXPAND_WILDCARDS,
        IndicesOptions.ConcreteTargetOptions.IGNORE_UNAVAILABLE,
        IndicesOptions.WildcardOptions.ALLOW_NO_INDICES,
        IndicesOptions.GatekeeperOptions.IGNORE_THROTTLED
    );
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Sets.addToCopy(SUPPORTED_QUERY_PARAMETERS, "index");

    public RestGetMappingAction() {}

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_mapping"),
            new Route(GET, "/_mappings"),
            new Route(GET, "/{index}/_mapping"),
            new Route(GET, "/{index}/_mappings")
        );
    }

    @Override
    public String getName() {
        return "get_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest(RestUtils.getMasterNodeTimeout(request));
        getMappingsRequest.indices(indices);
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        RestUtils.consumeDeprecatedLocalParameter(request);
        final HttpChannel httpChannel = request.getHttpChannel();
        return channel -> new RestCancellableNodeClient(client, httpChannel).admin()
            .indices()
            .getMappings(getMappingsRequest, new RestRefCountedChunkedToXContentListener<>(channel));
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
