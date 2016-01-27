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

package org.elasticsearch.rest.action.admin.cluster.repositories.get;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.client.Requests.getRepositoryRequest;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * Returns repository information
 */
public class RestGetRepositoriesAction extends BaseRestHandler {

    private final SettingsFilter settingsFilter;

    @Inject
    public RestGetRepositoriesAction(Settings settings, RestController controller, Client client, SettingsFilter settingsFilter) {
        super(settings, client);
        controller.registerHandler(GET, "/_snapshot", this);
        controller.registerHandler(GET, "/_snapshot/{repository}", this);
        this.settingsFilter = settingsFilter;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final String[] repositories = request.paramAsStringArray("repository", Strings.EMPTY_ARRAY);
        GetRepositoriesRequest getRepositoriesRequest = getRepositoryRequest(repositories);
        getRepositoriesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getRepositoriesRequest.masterNodeTimeout()));
        getRepositoriesRequest.local(request.paramAsBoolean("local", getRepositoriesRequest.local()));
        settingsFilter.addFilterSettingParams(request);
        client.admin().cluster().getRepositories(getRepositoriesRequest, new RestBuilderListener<GetRepositoriesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetRepositoriesResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                for (RepositoryMetaData repositoryMetaData : response.repositories()) {
                    RepositoriesMetaData.toXContent(repositoryMetaData, builder, request);
                }
                builder.endObject();

                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
