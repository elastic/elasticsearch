/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestIndexDeleteAliasesAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(DELETE, "/{index}/_alias/{name}"),
            new Route(DELETE, "/{index}/_aliases/{name}"));
    }

    @Override
    public String getName() {
        return "index_delete_aliases_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] aliases = Strings.splitStringByCommaToArray(request.param("name"));
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.timeout(request.paramAsTime("timeout", indicesAliasesRequest.timeout()));
        indicesAliasesRequest.addAliasAction(AliasActions.remove().indices(indices).aliases(aliases));
        indicesAliasesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", indicesAliasesRequest.masterNodeTimeout()));

        return channel -> client.admin().indices().aliases(indicesAliasesRequest, new RestToXContentListener<>(channel));
    }
}
