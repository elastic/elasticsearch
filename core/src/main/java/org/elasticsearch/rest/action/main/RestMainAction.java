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

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

/**
 *
 */
public class RestMainAction extends BaseRestHandler {

    private final Version version;
    private final ClusterName clusterName;
    private final ClusterService clusterService;

    @Inject
    public RestMainAction(Settings settings, Version version, RestController controller, ClusterName clusterName, Client client, ClusterService clusterService) {
        super(settings, controller, client);
        this.version = version;
        this.clusterName = clusterName;
        this.clusterService = clusterService;
        controller.registerHandler(GET, "/", this);
        controller.registerHandler(HEAD, "/", this);
    }

    @Override
    public void handleRequest(final RestRequest request, RestChannel channel, final Client client) throws Exception {

        RestStatus status = RestStatus.OK;
        if (clusterService.state().blocks().hasGlobalBlock(RestStatus.SERVICE_UNAVAILABLE)) {
            status = RestStatus.SERVICE_UNAVAILABLE;
        }
        if (request.method() == RestRequest.Method.HEAD) {
            channel.sendResponse(new BytesRestResponse(status));
            return;
        }

        XContentBuilder builder = channel.newBuilder();

        // Default to pretty printing, but allow ?pretty=false to disable
        if (!request.hasParam("pretty")) {
            builder.prettyPrint().lfAtEnd();
        }

        builder.startObject();
        if (settings.get("name") != null) {
            builder.field("name", settings.get("name"));
        }
        builder.field("cluster_name", clusterName.value());
        builder.startObject("version")
                .field("number", version.number())
                .field("build_hash", Build.CURRENT.hash())
                .field("build_timestamp", Build.CURRENT.timestamp())
                .field("build_snapshot", version.snapshot)
                .field("lucene_version", version.luceneVersion.toString())
                .endObject();
        builder.field("tagline", "You Know, for Search");
        builder.endObject();

        channel.sendResponse(new BytesRestResponse(status, builder));
    }
}
