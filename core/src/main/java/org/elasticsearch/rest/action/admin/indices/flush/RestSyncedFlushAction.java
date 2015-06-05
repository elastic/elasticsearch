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

package org.elasticsearch.rest.action.admin.indices.flush;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.flush.IndicesSyncedFlushResult;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 *
 */
public class RestSyncedFlushAction extends BaseRestHandler {

    private final SyncedFlushService syncedFlushService;

    @Inject
    public RestSyncedFlushAction(Settings settings, RestController controller, Client client, SyncedFlushService syncedFlushService) {
        super(settings, controller, client);
        this.syncedFlushService = syncedFlushService;
        controller.registerHandler(POST, "/_flush/synced", this);
        controller.registerHandler(POST, "/{index}/_flush/synced", this);

        controller.registerHandler(GET, "/_flush/synced", this);
        controller.registerHandler(GET, "/{index}/_flush/synced", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, IndicesOptions.lenientExpandOpen());

        syncedFlushService.attemptSyncedFlush(indices, indicesOptions, new RestBuilderListener<IndicesSyncedFlushResult>(channel) {
            @Override
            public RestResponse buildResponse(IndicesSyncedFlushResult results, XContentBuilder builder) throws Exception {
                builder.startObject();
                results.toXContent(builder, request);
                builder.endObject();
                return new BytesRestResponse(results.restStatus(), builder);
            }
        });
    }
}
