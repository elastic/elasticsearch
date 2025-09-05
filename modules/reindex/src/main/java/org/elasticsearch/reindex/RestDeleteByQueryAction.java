/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestDeleteByQueryAction extends AbstractBulkByQueryRestHandler<DeleteByQueryRequest, DeleteByQueryAction> {

    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestDeleteByQueryAction(Predicate<NodeFeature> clusterSupportsFeature) {
        super(DeleteByQueryAction.INSTANCE);
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_delete_by_query"));
    }

    @Override
    public String getName() {
        return "delete_by_query_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return doPrepareRequest(request, client, false, false);
    }

    @Override
    protected DeleteByQueryRequest buildRequest(RestRequest request) throws IOException {
        /*
         * Passing the search request through DeleteByQueryRequest first allows
         * it to set its own defaults which differ from SearchRequest's
         * defaults. Then the parseInternalRequest can override them.
         */
        DeleteByQueryRequest internal = new DeleteByQueryRequest();

        Map<String, Consumer<Object>> consumers = new HashMap<>();
        consumers.put("conflicts", o -> internal.setConflicts((String) o));
        consumers.put("max_docs", s -> setMaxDocsValidateIdentical(internal, ((Number) s).intValue()));

        parseInternalRequest(internal, request, clusterSupportsFeature, consumers);

        return internal;
    }
}
