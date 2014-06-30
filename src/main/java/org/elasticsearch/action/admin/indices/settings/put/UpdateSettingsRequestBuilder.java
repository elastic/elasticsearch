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

package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 * Builder for an update index settings request
 */
public class UpdateSettingsRequestBuilder extends AcknowledgedRequestBuilder<UpdateSettingsRequest, UpdateSettingsResponse, UpdateSettingsRequestBuilder, IndicesAdminClient> {

    public UpdateSettingsRequestBuilder(IndicesAdminClient indicesClient, String... indices) {
        super(indicesClient, new UpdateSettingsRequest(indices));
    }

    /**
     * Sets the indices the update settings will execute on
     */
    public UpdateSettingsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     *
     * For example indices that don't exist.
     */
    public UpdateSettingsRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequestBuilder setSettings(Settings settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the settings to be updated (either json/yaml/properties format)
     */
    public UpdateSettingsRequestBuilder setSettings(String source) {
        request.settings(source);
        return this;
    }

    /**
     * Sets the settings to be updated (either json/yaml/properties format)
     */
    public UpdateSettingsRequestBuilder setSettings(Map<String, Object> source) {
        request.settings(source);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<UpdateSettingsResponse> listener) {
        client.updateSettings(request, listener);
    }
}
