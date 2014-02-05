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

package org.elasticsearch.rest.action.admin.cluster.settings;

import org.elasticsearch.action.admin.cluster.settings.update.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.update.ClusterUpdateSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;

/**
 */
public class RestClusterUpdateSettingsAction extends BaseRestHandler {

    @Inject
    public RestClusterUpdateSettingsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.PUT, "/_cluster/settings", new PutHandler());
        controller.registerHandler(RestRequest.Method.POST, "/_cluster/settings", new PostHandler());
    }

    final class PutHandler implements RestHandler {
        @Override
        public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
            request.params().put("override", "true");
            RestClusterUpdateSettingsAction.this.handleRequest(request, channel);
        }
    }

    final class PostHandler implements RestHandler {
        @Override
        public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
            request.params().put("override", "false");
            RestClusterUpdateSettingsAction.this.handleRequest(request, channel);
        }
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) throws Exception {
        final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = Requests.clusterUpdateSettingsRequest();
        clusterUpdateSettingsRequest.listenerThreaded(false);
        clusterUpdateSettingsRequest.timeout(request.paramAsTime("timeout", clusterUpdateSettingsRequest.timeout()));
        clusterUpdateSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterUpdateSettingsRequest.masterNodeTimeout()));
        String sOverride = request.param("op_type");
        if (sOverride != null) {
            if ("true".equals(sOverride)) {
                clusterUpdateSettingsRequest.override(true);
            } else if ("false".equals(sOverride)) {
                clusterUpdateSettingsRequest.override(false);
            }
        }

        Map<String, Object> source = XContentFactory.xContent(request.content()).createParser(request.content()).mapAndClose();
        if (source.containsKey("transient")) {
            clusterUpdateSettingsRequest.transientSettings((Map) source.get("transient"));
        }
        if (source.containsKey("persistent")) {
            clusterUpdateSettingsRequest.persistentSettings((Map) source.get("persistent"));
        }

        client.admin().cluster().updateSettings(clusterUpdateSettingsRequest, new AcknowledgedRestListener<ClusterUpdateSettingsResponse>(channel) {
            @Override
            protected void addCustomFields(XContentBuilder builder, ClusterUpdateSettingsResponse response) throws IOException {
                builder.startObject("persistent");
                response.getPersistentSettings().toXContent(builder, request);
                builder.endObject();

                builder.startObject("transient");
                response.getTransientSettings().toXContent(builder, request);
                builder.endObject();
            }
        });
    }
}
