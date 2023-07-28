/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 * Builder for an update index settings request
 */
public class UpdateSettingsRequestBuilder extends AcknowledgedRequestBuilder<
    UpdateSettingsRequest,
    AcknowledgedResponse,
    UpdateSettingsRequestBuilder> {

    public UpdateSettingsRequestBuilder(ElasticsearchClient client, UpdateSettingsAction action, String... indices) {
        super(client, action, new UpdateSettingsRequest(indices));
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
     * <p>
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
     * Sets the settings to be updated
     */
    public UpdateSettingsRequestBuilder setSettings(Map<String, Object> source) {
        request.settings(source);
        return this;
    }

    public UpdateSettingsRequestBuilder setPreserveExisting(boolean preserveExisting) {
        request.setPreserveExisting(preserveExisting);
        return this;
    }

    /**
     * Sets the origin to use, only set this when the settings update is requested by ES internal processes.
     */
    public UpdateSettingsRequestBuilder origin(String origin) {
        request.origin(origin);
        return this;
    }
}
