/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestCountAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/count"), new Route(GET, "/_cat/count/{index}"));
    }

    @Override
    public String getName() {
        return "cat_count_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/count\n");
        sb.append("/_cat/count/{index}\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        SearchRequest countRequest = new SearchRequest(indices);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).trackTotalHits(true);
        countRequest.source(searchSourceBuilder);
        try {
            request.withContentOrSourceParamParserOrNull(parser -> {
                if (parser == null) {
                    QueryBuilder queryBuilder = RestActions.urlParamsToQueryBuilder(request);
                    if (queryBuilder != null) {
                        searchSourceBuilder.query(queryBuilder);
                    }
                } else {
                    searchSourceBuilder.query(RestActions.getQueryContent(parser));
                }
            });
        } catch (IOException e) {
            throw new ElasticsearchException("Couldn't parse query", e);
        }
        return channel -> client.search(countRequest, new RestResponseListener<SearchResponse>(channel) {
            @Override
            public RestResponse buildResponse(SearchResponse countResponse) throws Exception {
                assert countResponse.getHits().getTotalHits().relation == TotalHits.Relation.EQUAL_TO;
                return RestTable.buildResponse(buildTable(request, countResponse), channel);
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeadersWithTimestamp();
        table.addCell("count", "alias:dc,docs.count,docsCount;desc:the document count");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, SearchResponse response) {
        Table table = getTableWithHeader(request);
        table.startRow();
        table.addCell(response.getHits().getTotalHits().value);
        table.endRow();

        return table;
    }
}
