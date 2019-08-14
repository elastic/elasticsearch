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

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetStoredScriptAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestGetStoredScriptAction.class));

    public RestGetStoredScriptAction(RestController controller) {
        controller.registerHandler(GET, "/_script", this);
        controller.registerWithDeprecatedHandler(GET, "/_script/{name}", this,
            GET, "/_scripts/{name}", deprecationLogger);
        controller.registerHandler(HEAD, "/_script/{name}", this);
    }

    @Override
    public String getName() {
        return "get_stored_scripts_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        final String[] names = Strings.splitStringByCommaToArray(request.param("name"));

        GetStoredScriptRequest getRequest = new GetStoredScriptRequest(names);
        getRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getRequest.masterNodeTimeout()));

        final boolean implicitAll = getRequest.names().length == 0;

        return channel -> client.admin().cluster().getStoredScript(getRequest, new RestToXContentListener<>(channel) {
            @Override
            protected RestStatus getStatus(final GetStoredScriptResponse response)
            {
                Map<String, StoredScriptSource> storedScripts = response.getStoredScripts();
                final boolean templateExists = storedScripts != null && !storedScripts.isEmpty();
                return (templateExists || implicitAll) ? OK : NOT_FOUND;
            }
        });
    }
}
