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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.search.RemoteClusterService;
import org.elasticsearch.action.search.RemoteConnectionInfo;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public final class RestRemoteClusterInfoAction extends BaseRestHandler {

    private final RemoteClusterService remoteClusterService;

    public RestRemoteClusterInfoAction(Settings settings, RestController controller,
                                       RemoteClusterService remoteClusterService) {
        super(settings);
        controller.registerHandler(GET, "_remote/info", this);
        this.remoteClusterService = remoteClusterService;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client)
        throws IOException {
        return channel -> remoteClusterService.getRemoteConnectionInfos(
            new RestResponseListener<Collection<RemoteConnectionInfo>>(channel) {
            @Override
            public RestResponse buildResponse(
                Collection<RemoteConnectionInfo> remoteConnectionInfos) throws Exception {
                try (XContentBuilder xContentBuilder = channel.newBuilder()) {
                    xContentBuilder.startObject();
                    for (RemoteConnectionInfo info : remoteConnectionInfos) {
                        info.toXContent(xContentBuilder, request);
                    }
                    xContentBuilder.endObject();
                    return new BytesRestResponse(RestStatus.OK, xContentBuilder);
                }
            }
        });
    }
    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
