/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestGetDataSourceAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_query/data_source/{name}"), new Route(GET, "/_query/data_source"));
    }

    @Override
    public String getName() {
        return "esql_get_data_source";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final String[] requested = Strings.splitStringByCommaToArray(request.param("name"));
        final String[] names = Strings.isAllOrWildcard(requested) ? new String[] { "*" } : requested;
        final GetDataSourceAction.Request req = new GetDataSourceAction.Request(RestUtils.getMasterNodeTimeout(request), names);
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            GetDataSourceAction.INSTANCE,
            req,
            new RestToXContentListener<>(channel)
        );
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(EsqlDataSourcesCapabilities.DATA_SOURCES);
    }
}
