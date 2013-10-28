/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.Map;

/**
 */
public class RestClusterUpdateSettingsAction extends BaseRestHandler {

    @Inject
    public RestClusterUpdateSettingsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.PUT, "/_cluster/settings", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = Requests.clusterUpdateSettingsRequest();
        clusterUpdateSettingsRequest.listenerThreaded(false);
        clusterUpdateSettingsRequest.timeout(request.paramAsTime("timeout", clusterUpdateSettingsRequest.timeout()));
        clusterUpdateSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterUpdateSettingsRequest.masterNodeTimeout()));
        try {
            Map<String, Object> source = XContentFactory.xContent(request.content()).createParser(request.content()).mapAndClose();
            if (source.containsKey("transient")) {
                clusterUpdateSettingsRequest.transientSettings((Map) source.get("transient"));
            }
            if (source.containsKey("persistent")) {
                clusterUpdateSettingsRequest.persistentSettings((Map) source.get("persistent"));
            }
        } catch (Exception e) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.warn("Failed to send response", e1);
            }
            return;
        }

        client.admin().cluster().updateSettings(clusterUpdateSettingsRequest, new AcknowledgedRestResponseActionListener<ClusterUpdateSettingsResponse>(request, channel, logger) {

            @Override
            protected void addCustomFields(XContentBuilder builder, ClusterUpdateSettingsResponse response) throws IOException {
                builder.startObject("persistent");
                for (Map.Entry<String, String> entry : response.getPersistentSettings().getAsMap().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();

                builder.startObject("transient");
                for (Map.Entry<String, String> entry : response.getTransientSettings().getAsMap().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }

            @Override
            public void onFailure(Throwable e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to handle cluster state", e);
                }
                super.onFailure(e);
            }
        });
    }
}
