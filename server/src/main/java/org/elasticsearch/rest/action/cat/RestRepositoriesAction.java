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

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Cat API class to display information about snapshot repositories
 */
public class RestRepositoriesAction extends AbstractCatAction {
    public RestRepositoriesAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_cat/repositories", this);
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
        for (RepositoryMetaData repositoryMetaData : getRepositoriesResponse.repositories()) {
            table.startRow();

            table.addCell(repositoryMetaData.name());
            table.addCell(repositoryMetaData.type());

            table.endRow();
        }

        return table;
    }
}
