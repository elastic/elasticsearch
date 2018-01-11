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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.client.Requests.updateSettingsRequest;

public class RestUpdateSettingsAction extends BaseRestHandler {

    public RestUpdateSettingsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.PUT, "/{index}/_settings", this);
        controller.registerHandler(RestRequest.Method.PUT, "/_settings", this);
    }

    @Override
    public String getName() {
        return "update_settings_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        UpdateSettingsRequest updateSettingsRequest = updateSettingsRequest(Strings.splitStringByCommaToArray(request.param("index")));
        updateSettingsRequest.timeout(request.paramAsTime("timeout", updateSettingsRequest.timeout()));
        updateSettingsRequest.setPreserveExisting(request.paramAsBoolean("preserve_existing", updateSettingsRequest.isPreserveExisting()));
        updateSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", updateSettingsRequest.masterNodeTimeout()));
        updateSettingsRequest.indicesOptions(IndicesOptions.fromRequest(request, updateSettingsRequest.indicesOptions()));

        Map<String, Object> settings = new HashMap<>();
        try (XContentParser parser = request.contentParser()) {
            Map<String, Object> bodySettings = parser.map();
            Object innerBodySettings = bodySettings.get("settings");
            // clean up in case the body is wrapped with "settings" : { ... }
            if (innerBodySettings instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> innerBodySettingsMap = (Map<String, Object>) innerBodySettings;
                settings.putAll(innerBodySettingsMap);
            } else {
                settings.putAll(bodySettings);
            }
        }
        updateSettingsRequest.settings(settings);

        return channel -> client.admin().indices().updateSettings(updateSettingsRequest, new AcknowledgedRestListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }
}
