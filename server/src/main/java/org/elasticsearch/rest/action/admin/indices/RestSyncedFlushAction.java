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

import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSyncedFlushAction extends BaseRestHandler {

    public RestSyncedFlushAction(RestController controller) {
        controller.registerHandler(POST, "/_flush/synced", this);
        controller.registerHandler(POST, "/{index}/_flush/synced", this);

        controller.registerHandler(GET, "/_flush/synced", this);
        controller.registerHandler(GET, "/{index}/_flush/synced", this);
    }

    @Override
    public String getName() {
        return "synced_flush_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, IndicesOptions.lenientExpandOpen());
        SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest(Strings.splitStringByCommaToArray(request.param("index")));
        syncedFlushRequest.indicesOptions(indicesOptions);
        return channel -> client.admin().indices().syncedFlush(syncedFlushRequest, new RestBuilderListener<SyncedFlushResponse>(channel) {
            @Override
            public RestResponse buildResponse(SyncedFlushResponse results, XContentBuilder builder) throws Exception {
                builder.startObject();
                results.toXContent(builder, request);
                builder.endObject();
                return new BytesRestResponse(results.restStatus(), builder);
            }
        });
    }
}
