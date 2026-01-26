/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.rest;

import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestGetDataStreamsAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.copyOf(
        Sets.union(
            RestRequest.INTERNAL_MARKER_REQUEST_PARAMETERS,
            Set.of(
                "name",
                "include_defaults",
                "master_timeout",
                IndicesOptions.WildcardOptions.EXPAND_WILDCARDS,
                IndicesOptions.ConcreteTargetOptions.IGNORE_UNAVAILABLE,
                IndicesOptions.WildcardOptions.ALLOW_NO_INDICES,
                IndicesOptions.GatekeeperOptions.IGNORE_THROTTLED,
                "verbose"
            )
        )
    );
    public static final String FAILURES_LIFECYCLE_API_CAPABILITY = "failure_store.lifecycle";
    private static final Set<String> CAPABILITIES = Set.of(
        DataStreamLifecycle.EFFECTIVE_RETENTION_REST_API_CAPABILITY,
        FAILURES_LIFECYCLE_API_CAPABILITY,
        "failure_store.lifecycle.default_retention"
    );

    @Override
    public String getName() {
        return "get_data_streams_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_data_stream"), new Route(GET, "/_data_stream/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetDataStreamAction.Request getDataStreamsRequest = new GetDataStreamAction.Request(
            RestUtils.getMasterNodeTimeout(request),
            Strings.splitStringByCommaToArray(request.param("name"))
        );
        getDataStreamsRequest.includeDefaults(request.paramAsBoolean("include_defaults", false));
        getDataStreamsRequest.indicesOptions(IndicesOptions.fromRequest(request, getDataStreamsRequest.indicesOptions()));
        getDataStreamsRequest.verbose(request.paramAsBoolean("verbose", false));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            GetDataStreamAction.INSTANCE,
            getDataStreamsRequest,
            new RestToXContentListener<>(channel)
        );
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public Set<String> supportedCapabilities() {
        return CAPABILITIES;
    }

    @Override
    public Set<String> supportedQueryParameters() {
        return SUPPORTED_QUERY_PARAMETERS;
    }
}
