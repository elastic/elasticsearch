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
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.enterprisesearch.search.XSearchQueryBuilder;
import org.elasticsearch.xpack.enterprisesearch.search.XSearchQueryOptions;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {

        final String index = request.param("index");
        final String query = request.param("query");

        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(index);
        return channel -> client
            .admin()
            .indices()
            .getMappings(getMappingsRequest, new ActionListener<>() {
                @Override
                public void onResponse(GetMappingsResponse getMappingsResponse) {

                    final XSearchQueryOptions queryOptions = new XSearchQueryOptions(query, getMappingsResponse.mappings().values());
                    final QueryBuilder queryBuilder = XSearchQueryBuilder.getQueryBuilder(queryOptions);

                    SearchRequest searchRequest = client.prepareSearch(index)
                        .setQuery(queryBuilder)
                        .setSize(1000)
                        .setFetchSource(true)
                        .request();

                    client.execute(SearchAction.INSTANCE, searchRequest, new RestStatusToXContentListener<>(channel));
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
}
