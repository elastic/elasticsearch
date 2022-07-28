/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xpack.enterprisesearch.action;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.xpack.enterprisesearch.search.XSearchQueryBuilder;
import org.elasticsearch.xpack.enterprisesearch.search.XSearchQueryOptions;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class XSearchAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/{index}/_xsearch"));
    }

    @Override
    public String getName() {
        return "xsearch_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        QueryBuilder query = XSearchQueryBuilder.getQueryBuilder(new XSearchQueryOptions(request));

        final Supplier<ThreadContext.StoredContext> supplier = client.threadPool().getThreadContext().newRestorableContext(false);

        SearchRequest searchRequest = client.prepareSearch(request.param("index"))
            .setQuery(query)
            .setSize(1000)
            .setFetchSource(true)
            .request();

        return channel -> client.execute(SearchAction.INSTANCE, searchRequest, new RestStatusToXContentListener<>(channel));
    }
}
