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

package org.elasticsearch.rest.action.main;

import org.apache.lucene.util.Constants;
import org.elasticsearch.Build;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestResponseListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

/**
 *
 */
public class RestMainAction extends BaseRestHandler {

    private final Version version;

    @Inject
    public RestMainAction(Settings settings, Version version, Client client, RestController controller) {
        super(settings, client);
        this.version = version;
        controller.registerHandler(GET, "/", this);
        controller.registerHandler(HEAD, "/", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.listenerThreaded(false);
        clusterStateRequest.masterNodeTimeout(TimeValue.timeValueMillis(0));
        clusterStateRequest.local(true);
        clusterStateRequest.clear().blocks(true);
        client.admin().cluster().state(clusterStateRequest, new RestResponseListener<ClusterStateResponse>(channel) {
            @Override
            public RestResponse buildResponse(ClusterStateResponse response) throws Exception {
                RestStatus status = RestStatus.OK;
                if (response.getState().blocks().hasGlobalBlock(RestStatus.SERVICE_UNAVAILABLE)) {
                    status = RestStatus.SERVICE_UNAVAILABLE;
                }
                if (request.method() == RestRequest.Method.HEAD) {
                    return new BytesRestResponse(status);
                }

                XContentBuilder builder = channel.newBuilder();

                // Default to pretty printing, but allow ?pretty=false to disable
                if (!request.hasParam("pretty")) {
                    builder.prettyPrint().lfAtEnd();
                }

                builder.startObject();
                builder.field("status", status.getStatus());
                if (settings.get("name") != null) {
                    builder.field("name", settings.get("name"));
                }
                builder.startObject("version")
                        .field("number", version.number())
                        .field("build_hash", Build.CURRENT.hash())
                        .field("build_timestamp", Build.CURRENT.timestamp())
                        .field("build_snapshot", version.snapshot)
                                // We use the lucene version from lucene constants since
                                // this includes bugfix release version as well and is already in
                                // the right format. We can also be sure that the format is maitained
                                // since this is also recorded in lucene segments and has BW compat
                        .field("lucene_version", Constants.LUCENE_MAIN_VERSION)
                        .endObject();
                builder.field("tagline", "You Know, for Search");
                builder.endObject();
                return new BytesRestResponse(status, builder);
            }
        });
    }
}
