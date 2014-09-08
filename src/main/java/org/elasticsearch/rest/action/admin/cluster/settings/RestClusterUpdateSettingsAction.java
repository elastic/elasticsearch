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

import com.google.common.collect.Sets;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
        clusterUpdateSettingsRequest.listenerThreaded(false);
        clusterUpdateSettingsRequest.timeout(request.paramAsTime("timeout", clusterUpdateSettingsRequest.timeout()));
        clusterUpdateSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterUpdateSettingsRequest.masterNodeTimeout()));
        Map<String, Object> source = XContentFactory.xContent(request.content()).createParser(request.content()).mapAndClose();
        if (source.containsKey("transient")) {
            Map<String, Object> transientSettings = (Map) source.get("transient");
            Iterator<Map.Entry<String, Object>> iterator = transientSettings.entrySet().iterator();
            Set<String> transientSettingsToRemove = Sets.newHashSet();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                if (entry.getValue() == null) {
                    transientSettingsToRemove.add(entry.getKey());
                    iterator.remove();
                }
            }
            clusterUpdateSettingsRequest.transientSettings(transientSettings);
            clusterUpdateSettingsRequest.transientSettingsToRemove(transientSettingsToRemove);
        }
        if (source.containsKey("persistent")) {
            Map<String, Object> persistentSettings = (Map) source.get("persistent");
            Iterator<Map.Entry<String, Object>> iterator = persistentSettings.entrySet().iterator();
            Set<String> persistentSettingsToRemove = Sets.newHashSet();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                if (entry.getValue() == null) {
                    persistentSettingsToRemove.add(entry.getKey());
                    iterator.remove();
                }
            }
            clusterUpdateSettingsRequest.persistentSettings(persistentSettings);
            clusterUpdateSettingsRequest.persistentSettingsToRemove(persistentSettingsToRemove);
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
