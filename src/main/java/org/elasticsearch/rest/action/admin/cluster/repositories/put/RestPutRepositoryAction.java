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

package org.elasticsearch.rest.action.admin.cluster.repositories.put;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseActionRequestRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import static org.elasticsearch.client.Requests.putRepositoryRequest;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Registers repositories
 */
public class RestPutRepositoryAction extends BaseActionRequestRestHandler<PutRepositoryRequest> {

    @Inject
    public RestPutRepositoryAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(PUT, "/_snapshot/{repository}", this);
        controller.registerHandler(POST, "/_snapshot/{repository}", this);
    }

    @Override
    protected PutRepositoryRequest newRequest(RestRequest request) {
        PutRepositoryRequest putRepositoryRequest = putRepositoryRequest(request.param("repository"));
        putRepositoryRequest.listenerThreaded(false);
        putRepositoryRequest.source(request.content().toUtf8());
        putRepositoryRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putRepositoryRequest.masterNodeTimeout()));
        putRepositoryRequest.timeout(request.paramAsTime("timeout", putRepositoryRequest.timeout()));
        return putRepositoryRequest;
    }

    @Override
    public void doHandleRequest(final RestRequest restRequest, final RestChannel channel, PutRepositoryRequest request) {
        client.admin().cluster().putRepository(request, new AcknowledgedRestListener<PutRepositoryResponse>(channel));
    }
}
