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

import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetStoredScriptAction extends BaseRestHandler {

    private static final String NEW_FORMAT = "new_format";
    private static final Set<String> allowedResponseParameters = Collections
        .unmodifiableSet(Stream.concat(Collections.singleton(NEW_FORMAT).stream(), Settings.FORMAT_PARAMS.stream())
            .collect(Collectors.toSet()));

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_script"),
            new ReplacedRoute(
                GET, "/_script/{id}",
                GET, "/_scripts/{id}"));
    }

    @Override
    public String getName() {
        return "get_stored_script_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        final String[] names = Strings.splitStringByCommaToArray(request.param("id"));

        GetStoredScriptRequest getRequest = new GetStoredScriptRequest(names);
        getRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getRequest.masterNodeTimeout()));

        final boolean implicitAll = getRequest.ids().length == 0;

        return channel -> client.admin().cluster().getStoredScript(getRequest, new RestToXContentListener<>(channel) {
            @Override
            protected RestStatus getStatus(final GetStoredScriptResponse response)
            {
                Map<String, StoredScriptSource> storedScripts = response.getStoredScripts();
                final boolean scriptExists = storedScripts != null && !storedScripts.isEmpty();
                return (scriptExists || implicitAll) ? RestStatus.OK : RestStatus.NOT_FOUND;
            }
        });
    }

    @Override
    protected Set<String> responseParams() {
        return allowedResponseParameters;
    }
}
