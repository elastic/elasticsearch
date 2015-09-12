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

package org.elasticsearch.rest.action.admin.cluster.repositories.delete;

import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import static org.elasticsearch.client.Requests.deleteRepositoryRequest;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Unregisters a repository
 */
public class RestDeleteRepositoryAction extends BaseRestHandler {

    @Inject
    public RestDeleteRepositoryAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(DELETE, "/_snapshot/{repository}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        DeleteRepositoryRequest deleteRepositoryRequest = deleteRepositoryRequest(request.param("repository"));
        deleteRepositoryRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteRepositoryRequest.masterNodeTimeout()));
        deleteRepositoryRequest.timeout(request.paramAsTime("timeout", deleteRepositoryRequest.timeout()));
        deleteRepositoryRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteRepositoryRequest.masterNodeTimeout()));
        client.admin().cluster().deleteRepository(deleteRepositoryRequest, new AcknowledgedRestListener<DeleteRepositoryResponse>(channel));
    }
}
