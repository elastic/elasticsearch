/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Table;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Cat API class to display information about snapshot repositories
 */
public class RestRepositoriesAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/repositories"));
    }

    @Override
    protected RestChannelConsumer doCatRequest(RestRequest request, NodeClient client) {
        GetRepositoriesRequest getRepositoriesRequest = new GetRepositoriesRequest();
        getRepositoriesRequest.local(request.paramAsBoolean("local", getRepositoriesRequest.local()));
        getRepositoriesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getRepositoriesRequest.masterNodeTimeout()));

        return channel ->
                client.admin()
                        .cluster()
                        .getRepositories(getRepositoriesRequest, new RestResponseListener<GetRepositoriesResponse>(channel) {
                            @Override
                            public RestResponse buildResponse(GetRepositoriesResponse getRepositoriesResponse) throws Exception {
                                return RestTable.buildResponse(buildTable(request, getRepositoriesResponse), channel);
                            }
                        });
    }

    @Override
    public String getName() {
        return "cat_repositories_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/repositories\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        return new Table()
                .startHeaders()
                .addCell("id", "alias:id,repoId;desc:unique repository id")
                .addCell("type", "alias:t,type;text-align:right;desc:repository type")
                .endHeaders();
    }

    private Table buildTable(RestRequest req, GetRepositoriesResponse getRepositoriesResponse) {
        Table table = getTableWithHeader(req);
        for (RepositoryMetadata repositoryMetadata : getRepositoriesResponse.repositories()) {
            table.startRow();

            table.addCell(repositoryMetadata.name());
            table.addCell(repositoryMetadata.type());

            table.endRow();
        }

        return table;
    }
}
