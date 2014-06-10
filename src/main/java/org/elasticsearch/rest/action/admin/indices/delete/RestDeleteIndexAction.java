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

package org.elasticsearch.rest.action.admin.indices.delete;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseActionRequestRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

/**
 *
 */
public class RestDeleteIndexAction extends BaseActionRequestRestHandler<DeleteIndexRequest> {

    @Inject
    public RestDeleteIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.DELETE, "/", this);
        controller.registerHandler(RestRequest.Method.DELETE, "/{index}", this);
    }

    @Override
    protected DeleteIndexRequest newRequest(RestRequest request) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(Strings.splitStringByCommaToArray(request.param("index")));
        deleteIndexRequest.listenerThreaded(false);
        deleteIndexRequest.timeout(request.paramAsTime("timeout", deleteIndexRequest.timeout()));
        deleteIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteIndexRequest.masterNodeTimeout()));
        deleteIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, deleteIndexRequest.indicesOptions()));
        return deleteIndexRequest;
    }

    @Override
    public void doHandleRequest(final RestRequest restRequest, final RestChannel channel, DeleteIndexRequest request) {
        client.admin().indices().delete(request, new AcknowledgedRestListener<DeleteIndexResponse>(channel));
    }
}
