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

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.shrink.ShrinkRequest;
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
public class RestShrinkIndexAction extends BaseRestHandler {

    @Inject
    public RestShrinkIndexAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.PUT, "/{index}/_shrink/{target}", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_shrink/{target}", this);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        if (request.param("target") == null) {
            throw new IllegalArgumentException("no target index");
        }
        if (request.param("index") == null) {
            throw new IllegalArgumentException("no source index");
        }
        ShrinkRequest shrinkIndexRequest = new ShrinkRequest(request.param("target"), request.param("index"));
        if (request.hasContent()) {
            shrinkIndexRequest.source(request.content());
        }
        shrinkIndexRequest.timeout(request.paramAsTime("timeout", shrinkIndexRequest.timeout()));
        shrinkIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", shrinkIndexRequest.masterNodeTimeout()));
        client.admin().indices().shrinkIndex(shrinkIndexRequest, new AcknowledgedRestListener<>(channel));
    }
}
