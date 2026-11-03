/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.RetryActionRequest;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_PARAM;
import static org.elasticsearch.rest.RestUtils.REST_TIMEOUT_PARAM;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestRetryAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(
        REST_MASTER_TIMEOUT_PARAM,
        REST_TIMEOUT_PARAM,
        IndicesOptions.WildcardOptions.EXPAND_WILDCARDS,
        IndicesOptions.ConcreteTargetOptions.IGNORE_UNAVAILABLE,
        IndicesOptions.WildcardOptions.ALLOW_NO_INDICES,
        IndicesOptions.GatekeeperOptions.IGNORE_THROTTLED
    );
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Set.copyOf(Sets.union(SUPPORTED_QUERY_PARAMETERS, Set.of("index")));

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_ilm/retry"));
    }

    @Override
    public String getName() {
        return "ilm_retry_action";
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
    public Set<String> supportedCapabilities() {
        return Set.of("lookup_index_not_managed");
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        final var indices = Strings.splitStringByCommaToArray(restRequest.param("index"));
        final var request = new RetryActionRequest(getMasterNodeTimeout(restRequest), getAckTimeout(restRequest), indices);
        request.indices(indices);
        request.indicesOptions(IndicesOptions.fromRequest(restRequest, IndicesOptions.strictExpandOpen()));
        return channel -> client.execute(ILMActions.RETRY, request, new RestToXContentListener<>(channel));
    }
}
