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

import org.elasticsearch.action.admin.cluster.settings.delete.ClusterDeleteSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.delete.ClusterDeleteSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.io.IOException;

/**
 */
public class RestClusterDeleteSettingsAction extends BaseRestHandler {

    @Inject
    public RestClusterDeleteSettingsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.DELETE, "/_cluster/settings", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) throws Exception{
        final ClusterDeleteSettingsRequest clusterDeleteSettingsRequest = Requests.clusterDeleteSettingsRequest();
        clusterDeleteSettingsRequest.listenerThreaded(false);
        clusterDeleteSettingsRequest.timeout(request.paramAsTime("timeout", clusterDeleteSettingsRequest.timeout()));
        clusterDeleteSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterDeleteSettingsRequest.masterNodeTimeout()));

        if ("false".equals(request.param("delete_transient"))){
            clusterDeleteSettingsRequest.deleteTransient(false);
        }
        if ("false".equals(request.param("delete_persistent"))){
            clusterDeleteSettingsRequest.deletePersistent(false);
        }

        client.admin().cluster().deleteSettings(clusterDeleteSettingsRequest, new AcknowledgedRestListener<ClusterDeleteSettingsResponse>(channel) {

            @Override
            protected void addCustomFields(XContentBuilder builder, ClusterDeleteSettingsResponse response) throws IOException {
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
