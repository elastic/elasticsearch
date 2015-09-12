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

package org.elasticsearch.rest.action.admin.indices.mapping.put;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 *
 */
public class RestPutMappingAction extends BaseRestHandler {


    @Inject
    public RestPutMappingAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(PUT, "/{index}/_mapping/", this);
        controller.registerHandler(PUT, "/{index}/{type}/_mapping", this);
        controller.registerHandler(PUT, "/{index}/_mapping/{type}", this);
        controller.registerHandler(PUT, "/_mapping/{type}", this);

        controller.registerHandler(POST, "/{index}/_mapping/", this);
        controller.registerHandler(POST, "/{index}/{type}/_mapping", this);
        controller.registerHandler(POST, "/{index}/_mapping/{type}", this);
        controller.registerHandler(POST, "/_mapping/{type}", this);
        
        //register the same paths, but with plural form _mappings
        controller.registerHandler(PUT, "/{index}/_mappings/", this);
        controller.registerHandler(PUT, "/{index}/{type}/_mappings", this);
        controller.registerHandler(PUT, "/{index}/_mappings/{type}", this);
        controller.registerHandler(PUT, "/_mappings/{type}", this);

        controller.registerHandler(POST, "/{index}/_mappings/", this);
        controller.registerHandler(POST, "/{index}/{type}/_mappings", this);
        controller.registerHandler(POST, "/{index}/_mappings/{type}", this);
        controller.registerHandler(POST, "/_mappings/{type}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        PutMappingRequest putMappingRequest = putMappingRequest(Strings.splitStringByCommaToArray(request.param("index")));
        putMappingRequest.type(request.param("type"));
        putMappingRequest.source(request.content().toUtf8());
        putMappingRequest.updateAllTypes(request.paramAsBoolean("update_all_types", false));
        putMappingRequest.timeout(request.paramAsTime("timeout", putMappingRequest.timeout()));
        putMappingRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putMappingRequest.masterNodeTimeout()));
        putMappingRequest.indicesOptions(IndicesOptions.fromRequest(request, putMappingRequest.indicesOptions()));
        client.admin().indices().putMapping(putMappingRequest, new AcknowledgedRestListener<PutMappingResponse>(channel));
    }
}
