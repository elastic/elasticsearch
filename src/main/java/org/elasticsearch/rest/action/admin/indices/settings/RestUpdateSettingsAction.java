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

package org.elasticsearch.rest.action.admin.indices.settings;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.util.Map;

import static org.elasticsearch.client.Requests.updateSettingsRequest;
import com.google.common.collect.ImmutableSet;

/**
 *
 */
public class RestUpdateSettingsAction extends BaseRestHandler {

    private static final ImmutableSet<String> VALUES_TO_EXCLUDE = ImmutableSet.<String>builder()
            .add("pretty").add("timeout").add("master_timeout").add("index")
            .add("expand_wildcards").add("ignore_unavailable").add("allow_no_indices")
            .build();

    @Inject
    public RestUpdateSettingsAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.PUT, "/{index}/_settings", this);
        controller.registerHandler(RestRequest.Method.PUT, "/_settings", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        UpdateSettingsRequest updateSettingsRequest = updateSettingsRequest(Strings.splitStringByCommaToArray(request.param("index")));
        updateSettingsRequest.listenerThreaded(false);
        updateSettingsRequest.timeout(request.paramAsTime("timeout", updateSettingsRequest.timeout()));
        updateSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", updateSettingsRequest.masterNodeTimeout()));
        updateSettingsRequest.indicesOptions(IndicesOptions.fromRequest(request, updateSettingsRequest.indicesOptions()));

        ImmutableSettings.Builder updateSettings = ImmutableSettings.settingsBuilder();
        String bodySettingsStr = request.content().toUtf8();
        if (Strings.hasText(bodySettingsStr)) {
            Settings buildSettings = ImmutableSettings.settingsBuilder().loadFromSource(bodySettingsStr).build();
            for (Map.Entry<String, String> entry : buildSettings.getAsMap().entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                // clean up in case the body is wrapped with "settings" : { ... }
                if (key.startsWith("settings.")) {
                    key = key.substring("settings.".length());
                }
                updateSettings.put(key, value);
            }
        }
        for (Map.Entry<String, String> entry : request.params().entrySet()) {
            if (VALUES_TO_EXCLUDE.contains(entry.getKey())) {
                continue;
            }
            updateSettings.put(entry.getKey(), entry.getValue());
        }
        updateSettingsRequest.settings(updateSettings);

        client.admin().indices().updateSettings(updateSettingsRequest, new AcknowledgedRestListener<UpdateSettingsResponse>(channel));
    }
}
