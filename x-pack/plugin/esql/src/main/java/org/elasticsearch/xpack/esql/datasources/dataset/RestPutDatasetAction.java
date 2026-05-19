/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutDatasetAction extends BaseRestHandler {

    private final ClusterService clusterService;

    public RestPutDatasetAction(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_query/dataset/{name}"));
    }

    @Override
    public String getName() {
        return "esql_put_dataset";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String name = request.param("name");
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            PutDatasetAction.Request putRequest = PutDatasetAction.Request.fromXContent(
                parser,
                RestUtils.getMasterNodeTimeout(request),
                RestUtils.getAckTimeout(request),
                name
            );
            if (clusterService.state()
                .getMinTransportVersion()
                .supports(AuthorizeDatasetDatasourceAction.AUTHORIZE_DATASET_DATASOURCE_TRANSPORT_VERSION) == false) {
                return channel -> client.execute(PutDatasetAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
            }
            AuthorizeDatasetDatasourceAction.Request authRequest = AuthorizeDatasetDatasourceAction.Request.forDatasetPut(putRequest);
            return channel -> client.execute(
                AuthorizeDatasetDatasourceAction.INSTANCE,
                authRequest,
                new RestActionListener<AcknowledgedResponse>(channel) {
                    @Override
                    protected void processResponse(AcknowledgedResponse response) {
                        client.execute(PutDatasetAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
                    }
                }
            );
        }
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(EsqlDataSourcesCapabilities.DATA_SOURCES);
    }
}
