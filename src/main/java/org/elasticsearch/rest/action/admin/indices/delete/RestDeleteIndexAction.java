/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.rest.action.admin.indices.delete;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

/**
 *
 */
public class RestDeleteIndexAction extends BaseRestHandler {

    @Inject
    public RestDeleteIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.DELETE, "/", this);
        controller.registerHandler(RestRequest.Method.DELETE, "/{index}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(Strings.splitStringByCommaToArray(request.param("index")));
        deleteIndexRequest.listenerThreaded(false);
        deleteIndexRequest.timeout(request.paramAsTime("timeout", deleteIndexRequest.timeout()));
        deleteIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteIndexRequest.masterNodeTimeout()));
        client.admin().indices().delete(deleteIndexRequest, new AcknowledgedRestResponseActionListener<DeleteIndexResponse>(request, channel, logger));
    }
}
