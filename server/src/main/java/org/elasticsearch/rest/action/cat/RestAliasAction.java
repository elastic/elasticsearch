/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestAliasAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/aliases"), new Route(GET, "/_cat/aliases/{alias}"));
    }

    @Override
    public String getName() {
        return "cat_alias_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    protected RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final GetAliasesRequest getAliasesRequest = request.hasParam("alias")
            ? new GetAliasesRequest(Strings.commaDelimitedListToStringArray(request.param("alias")))
            : new GetAliasesRequest();
        getAliasesRequest.indicesOptions(IndicesOptions.fromRequest(request, getAliasesRequest.indicesOptions()));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .indices()
            .getAliases(getAliasesRequest, new RestResponseListener<>(channel) {
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

        for (Map.Entry<String, List<AliasMetadata>> cursor : response.getAliases().entrySet()) {
            String indexName = cursor.getKey();
            for (AliasMetadata aliasMetadata : cursor.getValue()) {
                table.startRow();
                table.addCell(aliasMetadata.alias());
                table.addCell(indexName);
                table.addCell(aliasMetadata.filteringRequired() ? "*" : "-");
                String indexRouting = Strings.hasLength(aliasMetadata.indexRouting()) ? aliasMetadata.indexRouting() : "-";
                table.addCell(indexRouting);
                String searchRouting = Strings.hasLength(aliasMetadata.searchRouting()) ? aliasMetadata.searchRouting() : "-";
                table.addCell(searchRouting);
                String isWriteIndex = aliasMetadata.writeIndex() == null ? "-" : aliasMetadata.writeIndex().toString();
                table.addCell(isWriteIndex);
                table.endRow();
            }
        }

        return table;
    }

}
