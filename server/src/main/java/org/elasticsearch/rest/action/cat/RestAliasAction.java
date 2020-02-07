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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestAliasAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_cat/aliases"),
            new Route(GET, "/_cat/aliases/{alias}")));
    }

    @Override
    public String getName() {
        return "cat_alias_action";
    }

    @Override
    protected RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final GetAliasesRequest getAliasesRequest = request.hasParam("alias") ?
                new GetAliasesRequest(Strings.commaDelimitedListToStringArray(request.param("alias"))) :
                new GetAliasesRequest();
        getAliasesRequest.local(request.paramAsBoolean("local", getAliasesRequest.local()));

        return channel -> client.admin().indices().getAliases(getAliasesRequest, new RestResponseListener<GetAliasesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetAliasesResponse response) throws Exception {
                Table tab = buildTable(request, response);
                return RestTable.buildResponse(tab, channel);
            }
        });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/aliases\n");
        sb.append("/_cat/aliases/{alias}\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        final Table table = new Table();
        table.startHeaders();
        table.addCell("alias", "alias:a;desc:alias name");
        table.addCell("index", "alias:i,idx;desc:index alias points to");
        table.addCell("filter", "alias:f,fi;desc:filter");
        table.addCell("routing.index", "alias:ri,routingIndex;desc:index routing");
        table.addCell("routing.search", "alias:rs,routingSearch;desc:search routing");
        table.addCell("is_write_index", "alias:w,isWriteIndex;desc:write index");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, GetAliasesResponse response) {
        Table table = getTableWithHeader(request);

        for (ObjectObjectCursor<String, List<AliasMetaData>> cursor : response.getAliases()) {
            String indexName = cursor.key;
            for (AliasMetaData aliasMetaData : cursor.value) {
                table.startRow();
                table.addCell(aliasMetaData.alias());
                table.addCell(indexName);
                table.addCell(aliasMetaData.filteringRequired() ? "*" : "-");
                String indexRouting = Strings.hasLength(aliasMetaData.indexRouting()) ? aliasMetaData.indexRouting() : "-";
                table.addCell(indexRouting);
                String searchRouting = Strings.hasLength(aliasMetaData.searchRouting()) ? aliasMetaData.searchRouting() : "-";
                table.addCell(searchRouting);
                String isWriteIndex = aliasMetaData.writeIndex() == null ? "-" : aliasMetaData.writeIndex().toString();
                table.addCell(isWriteIndex);
                table.endRow();
            }
        }

        return table;
    }

}
