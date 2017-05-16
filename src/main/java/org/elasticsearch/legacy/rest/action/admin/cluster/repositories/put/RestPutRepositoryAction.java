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

package org.elasticsearch.legacy.rest.action.admin.cluster.repositories.put;

import org.elasticsearch.legacy.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.legacy.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.legacy.client.Client;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.rest.*;
import org.elasticsearch.legacy.rest.action.support.AcknowledgedRestListener;

import static org.elasticsearch.legacy.client.Requests.putRepositoryRequest;
import static org.elasticsearch.legacy.rest.RestRequest.Method.POST;
import static org.elasticsearch.legacy.rest.RestRequest.Method.PUT;

/**
 * Registers repositories
 */
public class RestPutRepositoryAction extends BaseRestHandler {

    @Inject
    public RestPutRepositoryAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(PUT, "/_snapshot/{repository}", this);
        controller.registerHandler(POST, "/_snapshot/{repository}", this);
    }


    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        PutRepositoryRequest putRepositoryRequest = putRepositoryRequest(request.param("repository"));
        putRepositoryRequest.listenerThreaded(false);
        putRepositoryRequest.source(request.content().toUtf8());
        putRepositoryRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putRepositoryRequest.masterNodeTimeout()));
        putRepositoryRequest.timeout(request.paramAsTime("timeout", putRepositoryRequest.timeout()));
        client.admin().cluster().putRepository(putRepositoryRequest, new AcknowledgedRestListener<PutRepositoryResponse>(channel));
    }
}
