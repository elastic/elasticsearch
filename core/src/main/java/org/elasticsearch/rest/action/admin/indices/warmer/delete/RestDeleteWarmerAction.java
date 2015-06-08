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
package org.elasticsearch.rest.action.admin.indices.warmer.delete;

import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerRequest;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 */
public class RestDeleteWarmerAction extends BaseRestHandler {

    @Inject
    public RestDeleteWarmerAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(DELETE, "/{index}/_warmer", this);
        controller.registerHandler(DELETE, "/{index}/_warmer/{name}", this);
        controller.registerHandler(DELETE, "/{index}/_warmers", this);
        controller.registerHandler(DELETE, "/{index}/_warmers/{name}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        DeleteWarmerRequest deleteWarmerRequest = new DeleteWarmerRequest(Strings.splitStringByCommaToArray(request.param("name")))
                .indices(Strings.splitStringByCommaToArray(request.param("index")));
        deleteWarmerRequest.timeout(request.paramAsTime("timeout", deleteWarmerRequest.timeout()));
        deleteWarmerRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteWarmerRequest.masterNodeTimeout()));
        deleteWarmerRequest.indicesOptions(IndicesOptions.fromRequest(request, deleteWarmerRequest.indicesOptions()));
        client.admin().indices().deleteWarmer(deleteWarmerRequest, new AcknowledgedRestListener<DeleteWarmerResponse>(channel));
    }
}
