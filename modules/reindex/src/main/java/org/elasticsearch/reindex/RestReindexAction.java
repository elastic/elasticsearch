/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Expose reindex over rest.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestReindexAction extends AbstractBaseReindexRestHandler<ReindexRequest, ReindexAction> implements RestRequestFilter {

    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestReindexAction(Predicate<NodeFeature> clusterSupportsFeature) {
        super(ReindexAction.INSTANCE);
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_reindex"));
    }

    @Override
    public String getName() {
        return "reindex_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return doPrepareRequest(request, client, true, true);
    }

    @Override
    protected ReindexRequest buildRequest(RestRequest request) throws IOException {
        if (request.hasParam("pipeline")) {
            throw new IllegalArgumentException(
                "_reindex doesn't support [pipeline] as a query parameter. Specify it in the [dest] object instead."
            );
        }

        ReindexRequest internal;
        try (XContentParser parser = request.contentParser()) {
            internal = ReindexRequest.fromXContent(parser, clusterSupportsFeature);
        }

        if (request.hasParam("scroll")) {
            internal.setScroll(parseTimeValue(request.param("scroll"), "scroll"));
        }
        if (request.hasParam(DocWriteRequest.REQUIRE_ALIAS)) {
            internal.setRequireAlias(request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, false));
        }

        return internal;
    }

    private static final Set<String> FILTERED_FIELDS = Set.of("source.remote.host.password");

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}
