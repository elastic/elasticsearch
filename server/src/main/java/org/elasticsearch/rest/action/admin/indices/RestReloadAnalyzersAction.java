/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersRequest;
import org.elasticsearch.action.admin.indices.analyze.TransportReloadAnalyzersAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestReloadAnalyzersAction extends BaseRestHandler {

    /**
     * Capability advertising that this node shares analyzer instances across indices with identical
     * analysis recipes, so reloading one index converges every index backed by the same shared instance.
     * Only present when the {@code shared_analyzers} feature flag is enabled.
     */
    public static final String SHARED_ANALYZERS = "shared_analyzers";

    private static final Set<String> CAPABILITIES = AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled()
        ? Set.of(SHARED_ANALYZERS)
        : Set.of();

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_reload_search_analyzers"), new Route(POST, "/{index}/_reload_search_analyzers"));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return CAPABILITIES;
    }

    @Override
    public String getName() {
        return "reload_search_analyzers_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ReloadAnalyzersRequest reloadAnalyzersRequest = new ReloadAnalyzersRequest(
            request.param("resource"),
            request.paramAsBoolean("preview", false),
            Strings.splitStringByCommaToArray(request.param("index"))
        );
        reloadAnalyzersRequest.indicesOptions(IndicesOptions.fromRequest(request, reloadAnalyzersRequest.indicesOptions()));
        return channel -> client.execute(
            TransportReloadAnalyzersAction.TYPE,
            reloadAnalyzersRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
