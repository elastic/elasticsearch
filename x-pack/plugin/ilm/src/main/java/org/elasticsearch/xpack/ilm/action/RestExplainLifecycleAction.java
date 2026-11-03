/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_PARAM;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestExplainLifecycleAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(
        REST_MASTER_TIMEOUT_PARAM,
        IndicesOptions.WildcardOptions.EXPAND_WILDCARDS,
        IndicesOptions.ConcreteTargetOptions.IGNORE_UNAVAILABLE,
        IndicesOptions.WildcardOptions.ALLOW_NO_INDICES,
        IndicesOptions.GatekeeperOptions.IGNORE_THROTTLED,
        "only_managed",
        "only_errors"
    );
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Set.copyOf(Sets.union(SUPPORTED_QUERY_PARAMETERS, Set.of("index")));

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_ilm/explain"));
    }

    @Override
    public String getName() {
        return "ilm_explain_action";
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
        return Set.of("explain_unmanaged_reason");
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String[] indexes = Strings.splitStringByCommaToArray(restRequest.param("index"));
        ExplainLifecycleRequest explainLifecycleRequest = new ExplainLifecycleRequest(getMasterNodeTimeout(restRequest));
        explainLifecycleRequest.indices(indexes);
        explainLifecycleRequest.indicesOptions(IndicesOptions.fromRequest(restRequest, IndicesOptions.strictExpandOpen()));
        explainLifecycleRequest.onlyManaged(restRequest.paramAsBoolean("only_managed", false));
        explainLifecycleRequest.onlyErrors(restRequest.paramAsBoolean("only_errors", false));
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            ExplainLifecycleAction.INSTANCE,
            explainLifecycleRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
