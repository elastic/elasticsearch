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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 *
 */
public class RestAliasAction extends AbstractCatAction {

    @Inject
    public RestAliasAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_cat/aliases", this);
        controller.registerHandler(GET, "/_cat/aliases/{alias}", this);
    }


    @Override
    void doRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final GetAliasesRequest getAliasesRequest = request.hasParam("alias") ?
                new GetAliasesRequest(request.param("alias")) :
                new GetAliasesRequest();
        getAliasesRequest.local(request.paramAsBoolean("local", getAliasesRequest.local()));

        client.admin().indices().getAliases(getAliasesRequest, new RestResponseListener<GetAliasesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetAliasesResponse response) throws Exception {
                Table tab = buildTable(request, response);
                return RestTable.buildResponse(tab, channel);
            }
        });
    }

    @Override
    void documentation(StringBuilder sb) {
        sb.append("/_cat/aliases\n");
        sb.append("/_cat/aliases/{alias}\n");
    }

    @Override
    Table getTableWithHeader(RestRequest request) {
        final Table table = new Table();
        table.startHeaders();
        table.addCell("alias", "alias:a;desc:alias name");
        table.addCell("index", "alias:i,idx;desc:index alias points to");
        table.addCell("filter", "alias:f,fi;desc:filter");
        table.addCell("routing.index", "alias:ri,routingIndex;desc:index routing");
        table.addCell("routing.search", "alias:rs,routingSearch;desc:search routing");
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
                table.endRow();
            }
        }

        return table;
    }

}
