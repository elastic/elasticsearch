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

import java.util.Arrays;

/**
 * Cluster state update request that allows to update settings for some indices
 */
public class UpdateSettingsClusterStateUpdateRequest extends IndicesClusterStateUpdateRequest<UpdateSettingsClusterStateUpdateRequest> {

    private Settings settings;

    private boolean preserveExisting = false;

    private boolean reopenShards = false;

    /**
     * Returns <code>true</code> iff the settings update should only add but not update settings. If the setting already exists
     * it should not be overwritten by this update. The default is <code>false</code>
     */
    public boolean isPreserveExisting() {
        return preserveExisting;
    }

    /**
     * Returns <code>true</code> if non-dynamic setting updates should go through, by automatically unassigning shards in the same cluster
     * state change as the setting update. The shards will be automatically reassigned after the cluster state update is made. The
     * default is <code>false</code>.
     */
    public boolean reopenShards() {
        return reopenShards;
    }

    public UpdateSettingsClusterStateUpdateRequest reopenShards(boolean reopenShards) {
        this.reopenShards = reopenShards;
        return this;
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

    @Override
    public String toString() {
        return Arrays.toString(indices()) + settings;
    }
}
