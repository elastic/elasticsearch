/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Expose reindex over rest.
 */
public class RestReindexAction extends AbstractBaseReindexRestHandler<ReindexRequest, ReindexAction> implements RestRequestFilter {

    public RestReindexAction() {
        super(ReindexAction.INSTANCE);
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
    protected ReindexRequest buildRequest(RestRequest request, NamedWriteableRegistry namedWriteableRegistry) throws IOException {
        if (request.hasParam("pipeline")) {
            throw new IllegalArgumentException(
                "_reindex doesn't support [pipeline] as a query parameter. Specify it in the [dest] object instead."
            );
        }

        ReindexRequest internal;
        try (XContentParser parser = request.contentParser()) {
            internal = ReindexRequest.fromXContent(parser);
        }

        if (request.hasParam("scroll")) {
            internal.setScroll(parseTimeValue(request.param("scroll"), "scroll"));
        }
        if (request.hasParam(DocWriteRequest.REQUIRE_ALIAS)) {
            internal.setRequireAlias(request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, false));
        }

        // Note that ReindexRequest is created with default IndicesOptions
        // respect other indices options (like ignore_unavailable) which can come via request parameters
        internal.getSearchRequest().indicesOptions(IndicesOptions.fromRequest(request, internal.getSearchRequest().indicesOptions()));

        return internal;
    }

    private static final Set<String> FILTERED_FIELDS = Set.of("source.remote.host.password");

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}
