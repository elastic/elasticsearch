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

package org.elasticsearch.rest.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

/**
 *
 */
public class RestCreateIndexAction extends BaseRestHandler {

    @Inject
    public RestCreateIndexAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.PUT, "/{index}", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}", this);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(request.param("index"));
        if (request.hasContent()) {
            createIndexRequest.source(request.content());
        }
        createIndexRequest.updateAllTypes(request.paramAsBoolean("update_all_types", false));
        createIndexRequest.timeout(request.paramAsTime("timeout", createIndexRequest.timeout()));
        createIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", createIndexRequest.masterNodeTimeout()));
        client.admin().indices().create(createIndexRequest, new AcknowledgedRestListener<CreateIndexResponse>(channel));
    }
}
