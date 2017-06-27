/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestCountAction extends AbstractCatAction {
    public RestCountAction(Settings settings, RestController restController) {
        super(settings);
        restController.registerHandler(GET, "/_cat/count", this);
        restController.registerHandler(GET, "/_cat/count/{index}", this);
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
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0);
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
        table.addCell(response.getHits().getTotalHits());
        table.endRow();

        return table;
    }
}
