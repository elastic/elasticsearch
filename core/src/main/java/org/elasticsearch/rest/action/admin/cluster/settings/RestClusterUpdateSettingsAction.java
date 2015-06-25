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

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.io.IOException;
import java.util.Map;

/**
 */
public class RestClusterUpdateSettingsAction extends BaseRestHandler {

    @Inject
    public RestClusterUpdateSettingsAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.PUT, "/_cluster/settings", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = Requests.clusterUpdateSettingsRequest();
        clusterUpdateSettingsRequest.timeout(request.paramAsTime("timeout", clusterUpdateSettingsRequest.timeout()));
        clusterUpdateSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterUpdateSettingsRequest.masterNodeTimeout()));
        Map<String, Object> source;
        try (XContentParser parser = XContentFactory.xContent(request.content()).createParser(request.content())) {
            source = parser.map();
        }
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
