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

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.client.Requests.updateSettingsRequest;
import static org.elasticsearch.common.util.set.Sets.newHashSet;

/**
 *
 */
public class RestUpdateSettingsAction extends BaseRestHandler {
    private static final Set<String> VALUES_TO_EXCLUDE = unmodifiableSet(newHashSet(
            "pretty",
            "timeout",
            "master_timeout",
            "index",
            "preserve_existing",
            "expand_wildcards",
            "ignore_unavailable",
            "allow_no_indices"));

    @Inject
    public RestUpdateSettingsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.PUT, "/{index}/_settings", this);
        controller.registerHandler(RestRequest.Method.PUT, "/_settings", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) {
        UpdateSettingsRequest updateSettingsRequest = updateSettingsRequest(Strings.splitStringByCommaToArray(request.param("index")));
        updateSettingsRequest.timeout(request.paramAsTime("timeout", updateSettingsRequest.timeout()));
        updateSettingsRequest.setPreserveExisting(request.paramAsBoolean("preserve_existing", updateSettingsRequest.isPreserveExisting()));
        updateSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", updateSettingsRequest.masterNodeTimeout()));
        updateSettingsRequest.indicesOptions(IndicesOptions.fromRequest(request, updateSettingsRequest.indicesOptions()));

        Settings.Builder updateSettings = Settings.builder();
        String bodySettingsStr = request.content().utf8ToString();
        if (Strings.hasText(bodySettingsStr)) {
            Settings buildSettings = Settings.builder().loadFromSource(bodySettingsStr).build();
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

        client.admin().indices().updateSettings(updateSettingsRequest, new AcknowledgedRestListener<>(channel));
    }
}
