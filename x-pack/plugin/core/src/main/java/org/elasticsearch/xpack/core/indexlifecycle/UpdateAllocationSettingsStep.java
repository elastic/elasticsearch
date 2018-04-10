/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.Map;
import java.util.Objects;

public class UpdateAllocationSettingsStep extends AsyncActionStep {
    public static final String NAME = "update-allocation";

    private final Map<String, String> include;
    private final Map<String, String> exclude;
    private final Map<String, String> require;

    public UpdateAllocationSettingsStep(StepKey key, StepKey nextStepKey, Client client, Map<String, String> include,
                                        Map<String, String> exclude, Map<String, String> require) {
        super(key, nextStepKey, client);
        this.include = include;
        this.exclude = exclude;
        this.require = require;
    }

    @Override
    public void performAction(Index index, Listener listener) {
        Settings.Builder newSettings = Settings.builder();
        include.forEach((key, value) -> newSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + key, value));
        exclude.forEach((key, value) -> newSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + key, value));
        require.forEach((key, value) -> newSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + key, value));

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index.getName()).settings(newSettings);
        getClient().admin().indices().updateSettings(updateSettingsRequest,
                ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
    }
    
    Map<String, String> getInclude() {
        return include;
    }

    Map<String, String> getExclude() {
        return exclude;
    }

    Map<String, String> getRequire() {
        return require;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), include, exclude, require);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UpdateAllocationSettingsStep other = (UpdateAllocationSettingsStep) obj;
        return super.equals(obj) &&
                Objects.equals(include, other.include) &&
                Objects.equals(exclude, other.exclude) &&
                Objects.equals(require, other.require);
    }
}
