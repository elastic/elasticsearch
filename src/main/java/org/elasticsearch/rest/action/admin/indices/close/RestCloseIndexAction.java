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

package org.elasticsearch.rest.action.admin.indices.close;

import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

/**
 *
 */
public class RestCloseIndexAction extends BaseRestHandler {

    @Inject
    public RestCloseIndexAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.POST, "/_close", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_close", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(Strings.splitStringByCommaToArray(request.param("index")));
        closeIndexRequest.listenerThreaded(false);
        closeIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", closeIndexRequest.masterNodeTimeout()));
        closeIndexRequest.timeout(request.paramAsTime("timeout", closeIndexRequest.timeout()));
        closeIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, closeIndexRequest.indicesOptions()));
        client.admin().indices().close(closeIndexRequest, new AcknowledgedRestListener<CloseIndexResponse>(channel));
    }
}
