/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.cluster.ack.IndicesClusterStateUpdateRequest;
import org.elasticsearch.common.settings.Settings;

/**
 * Cluster state update request that allows to update settings for some indices
 */
public class UpdateSettingsClusterStateUpdateRequest extends IndicesClusterStateUpdateRequest<UpdateSettingsClusterStateUpdateRequest> {

    private Settings settings;

    private boolean preserveExisting = false;

    /**
     * Returns <code>true</code> iff the settings update should only add but not update settings. If the setting already exists
     * it should not be overwritten by this update. The default is <code>false</code>
     */
    public boolean isPreserveExisting() {
        return preserveExisting;
    }

    /**
     * Iff set to <code>true</code> this settings update will only add settings not already set on an index. Existing settings remain
     * unchanged.
     */
    public UpdateSettingsClusterStateUpdateRequest setPreserveExisting(boolean preserveExisting) {
        this.preserveExisting = preserveExisting;
        return this;
    }

    /**
     * Returns the {@link Settings} to update
     */
    public Settings settings() {
        return settings;
    }

    /**
     * Sets the {@link Settings} to update
     */
    public UpdateSettingsClusterStateUpdateRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }
}
