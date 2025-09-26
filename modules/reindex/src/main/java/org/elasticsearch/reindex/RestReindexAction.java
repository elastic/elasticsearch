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
import org.elasticsearch.rest.FilteredRestRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    /**
     * This method isn't used because we implement {@link #getFilteredRequest(RestRequest)} instead
     */
    @Override
    public Set<String> getFilteredFields() {
        assert false : "This method should never be called";
        throw new UnsupportedOperationException();
    }

    @Override
    public RestRequest getFilteredRequest(RestRequest restRequest) {
        if (restRequest.hasContent()) {
            return new FilteredRestRequest(restRequest, Set.of()) {
                @Override
                @SuppressWarnings({ "rawtypes", "unchecked" })
                protected Map<String, Object> transformBody(Map<String, Object> map) {
                    final var source = map.get("source");
                    if (source instanceof Map sourceMap) {
                        final var remote = sourceMap.get("remote");
                        if (remote instanceof Map remoteMap) {
                            remoteMap.computeIfPresent("password", (key, value) -> "::es-redacted::");
                            remoteMap.computeIfPresent("headers", (key, value) -> {
                                if (value instanceof Map<?, ?> headers) {
                                    return headers.entrySet()
                                        .stream()
                                        .collect(Collectors.toMap(Map.Entry::getKey, ignore -> "::es-redacted::"));
                                } else {
                                    return null;
                                }
                            });
                        }
                    }
                    return map;
                }
            };
        } else {
            return restRequest;
        }
    }
}
