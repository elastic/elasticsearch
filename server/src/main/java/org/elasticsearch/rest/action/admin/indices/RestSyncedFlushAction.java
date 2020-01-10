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

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;

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
        FlushRequest flushRequest = new FlushRequest(Strings.splitStringByCommaToArray(request.param("index")));
        flushRequest.indicesOptions(IndicesOptions.fromRequest(request, flushRequest.indicesOptions()));
        return channel -> client.admin().indices().flush(flushRequest, new SimulateSyncedFlushResponseListener(channel));
    }

    static final class SimulateSyncedFlushResponseListener extends RestToXContentListener<FlushResponse> {
        SimulateSyncedFlushResponseListener(RestChannel channel) {
            super(channel);
        }

        @Override
        public RestResponse buildResponse(FlushResponse flushResponse, XContentBuilder builder) throws Exception {
            final RestStatus restStatus = flushResponse.getFailedShards() > 0 ? RestStatus.OK : RestStatus.CONFLICT;
            final SyncedFlushResponse syncedFlushResponse = new SyncedFlushResponse(flushResponse);
            syncedFlushResponse.toXContent(builder, channel.request());
            return new BytesRestResponse(restStatus, builder);
        }
    }

    static final class SyncedFlushResponse implements ToXContent {
        private final FlushResponse flushResponse;

        SyncedFlushResponse(FlushResponse flushResponse) {
            this.flushResponse = flushResponse;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("_shards");
            builder.field("warning", "Synced flush was removed and a normal flush was performed instead. "
                + "This transition will be removed a future Elasticsearch version.");
            builder.field("total", flushResponse.getTotalShards());
            builder.field("successful", flushResponse.getSuccessfulShards());
            builder.field("failed", flushResponse.getFailedShards());
            // We can't serialize the detail of each index as we don't have the shard count per index.
            builder.endObject();
            return builder;
        }
    }
}
