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

package org.elasticsearch.rest.action.admin.indices.syncedflush;

import org.elasticsearch.action.admin.indices.syncedflush.SyncedFlushIndicesAction;
import org.elasticsearch.action.admin.indices.syncedflush.SyncedFlushIndicesRequest;
import org.elasticsearch.action.admin.indices.syncedflush.SyncedFlushIndicesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.SyncedFlushService;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestSyncedFlushAction extends BaseRestHandler {

    @Inject
    public RestSyncedFlushAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_syncedflush", this);
        controller.registerHandler(POST, "/{index}/_syncedflush", this);

        controller.registerHandler(GET, "/_syncedflush", this);
        controller.registerHandler(GET, "/{index}/_syncedflush", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        SyncedFlushIndicesRequest syncedFlushIndicesRequest = new SyncedFlushIndicesRequest(indices);
        client.admin().indices().execute(SyncedFlushIndicesAction.INSTANCE, syncedFlushIndicesRequest, new RestBuilderListener<SyncedFlushIndicesResponse>(channel) {
            @Override
            public RestResponse buildResponse(SyncedFlushIndicesResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder = response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
