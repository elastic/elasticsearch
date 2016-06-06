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

import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestRolloverIndexAction extends BaseRestHandler {

    @Inject
    public RestRolloverIndexAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.PUT, "/{alias}/_rollover", this);
        controller.registerHandler(RestRequest.Method.POST, "/{alias}/_rollover", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        if (request.param("alias") == null) {
            throw new IllegalArgumentException("no source alias");
        }
        RolloverRequest rolloverIndexRequest = new RolloverRequest(request.param("alias"));
        if (request.hasContent()) {
            rolloverIndexRequest.source(request.content());
        }
        rolloverIndexRequest.timeout(request.paramAsTime("timeout", rolloverIndexRequest.timeout()));
        rolloverIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", rolloverIndexRequest.masterNodeTimeout()));
        client.admin().indices().rolloverIndex(rolloverIndexRequest, new RestBuilderListener<RolloverResponse>(channel) {
            @Override
            public RestResponse buildResponse(RolloverResponse rolloverResponse, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("old_index", rolloverResponse.getOldIndex());
                builder.field("new_index", rolloverResponse.getNewIndex());
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
