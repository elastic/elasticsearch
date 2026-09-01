/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestEnableDatasetAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query/dataset/{name}/_enable"));
    }

    @Override
    public String getName() {
        return "esql_enable_dataset";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final EnableDatasetAction.Request req = new EnableDatasetAction.Request(
            RestUtils.getMasterNodeTimeout(request),
            RestUtils.getAckTimeout(request),
            Strings.splitStringByCommaToArray(request.param("name"))
        );
        return channel -> client.execute(EnableDatasetAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(
            EsqlDataSourcesCapabilities.DATA_SOURCES,
            EsqlDataSourcesCapabilities.DATA_SOURCES_SERVERLESS_SCOPE,
            EsqlDataSourcesCapabilities.DATA_SOURCE_DATASET_ENABLE_DISABLE
        );
    }
}
