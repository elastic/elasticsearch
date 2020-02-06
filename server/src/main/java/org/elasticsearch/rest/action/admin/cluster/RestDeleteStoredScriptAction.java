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

import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteStoredScriptAction extends BaseRestHandler {

    public RestDeleteStoredScriptAction(RestController controller) {
        controller.registerHandler(DELETE, "/_scripts/{id}", this);
    }

    @Override
    public String getName() {
        return "delete_stored_script_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String id = request.param("id");
        DeleteStoredScriptRequest deleteStoredScriptRequest = new DeleteStoredScriptRequest(id);
        deleteStoredScriptRequest.timeout(request.paramAsTime("timeout", deleteStoredScriptRequest.timeout()));
        deleteStoredScriptRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteStoredScriptRequest.masterNodeTimeout()));

        return channel -> client.admin().cluster().deleteStoredScript(deleteStoredScriptRequest, new RestToXContentListener<>(channel));
    }
}
