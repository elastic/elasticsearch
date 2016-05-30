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

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestCountAction extends AbstractCatAction {

    private final IndicesQueriesRegistry indicesQueriesRegistry;

    @Inject
    public RestCountAction(Settings settings, RestController restController, RestController controller, Client client, IndicesQueriesRegistry indicesQueriesRegistry) {
        super(settings, controller, client);
        restController.registerHandler(GET, "/_cat/count", this);
        restController.registerHandler(GET, "/_cat/count/{index}", this);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/count\n");
        sb.append("/_cat/count/{index}\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        SearchRequest countRequest = new SearchRequest(indices);
        String source = request.param("source");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0);
        countRequest.source(searchSourceBuilder);
        if (source != null) {
            searchSourceBuilder.query(RestActions.getQueryContent(new BytesArray(source), indicesQueriesRegistry, parseFieldMatcher));
        } else {
            QueryBuilder queryBuilder = RestActions.urlParamsToQueryBuilder(request);
            if (queryBuilder != null) {
                searchSourceBuilder.query(queryBuilder);
            }
        }
        client.search(countRequest, new RestResponseListener<SearchResponse>(channel) {
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
        table.addCell(response.getHits().totalHits());
        table.endRow();

        return table;
    }
}
