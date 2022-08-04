/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xpack.enterprisesearch.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.enterprisesearch.search.XSearchQueryBuilder;
import org.elasticsearch.xpack.enterprisesearch.search.XSearchQueryOptions;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;

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

    private static void performSearch(XSearchQueryOptions queryOptions, NodeClient client, RestChannel channel) {

        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(queryOptions.getIndex());

        client
            .admin()
            .indices()
            .getMappings(getMappingsRequest, new ActionListener<>() {
                @Override
                public void onResponse(GetMappingsResponse getMappingsResponse) {
                    queryOptions.setSearchFields(getMappingsResponse.mappings().values());
                    performQuery(queryOptions, client, channel);
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(new RestResponse(channel, BAD_REQUEST, e));
                    } catch (final IOException ex) {
                        throw new AssertionError(e);
                    }
                }
            });
    }

    private static void performQuery(XSearchQueryOptions queryOptions, NodeClient client, RestChannel channel) {
        final QueryBuilder queryBuilder = XSearchQueryBuilder.getQueryBuilder(queryOptions);

        SearchRequest searchRequest = client.prepareSearch(queryOptions.getIndex())
            .setQuery(queryBuilder)
            .setSize(1000)
            .setFetchSource(true)
            .request();

        client.execute(SearchAction.INSTANCE, searchRequest, new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(SearchResponse searchResponse, XContentBuilder builder) throws Exception {
                XSearchResponseBuilder.buildResponse(searchResponse, builder);
                return new RestResponse(RestStatus.OK, builder);
            }
        });
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {

        XSearchQueryOptions queryOptions = new XSearchQueryOptions();
        queryOptions.setIndex(request.param("index"));
        queryOptions.setQuery(request.param("query"));

        return channel -> performSearch(queryOptions, client, channel);
    }


}
