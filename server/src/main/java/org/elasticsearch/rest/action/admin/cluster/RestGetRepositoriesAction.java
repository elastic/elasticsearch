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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.client.Requests.getRepositoryRequest;
import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Returns repository information
 */
public class RestGetRepositoriesAction extends BaseRestHandler {

    private final SettingsFilter settingsFilter;

    public RestGetRepositoriesAction(SettingsFilter settingsFilter) {
        this.settingsFilter = settingsFilter;
    }

    @Override
    public String getName() {
        return "get_repositories_action";
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_snapshot"),
            new Route(GET, "/_snapshot/{repository}")));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] repositories = request.paramAsStringArray("repository", Strings.EMPTY_ARRAY);
        GetRepositoriesRequest getRepositoriesRequest = getRepositoryRequest(repositories);
        getRepositoriesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getRepositoriesRequest.masterNodeTimeout()));
        getRepositoriesRequest.local(request.paramAsBoolean("local", getRepositoriesRequest.local()));
        settingsFilter.addFilterSettingParams(request);
        return channel ->
                client.admin().cluster().getRepositories(getRepositoriesRequest,
                    new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

}
