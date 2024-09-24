/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.cluster.ack.IndicesClusterStateUpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;

import java.util.Arrays;

/**
 * Cluster state update request that allows to update settings for some indices
 */
public class UpdateSettingsClusterStateUpdateRequest extends IndicesClusterStateUpdateRequest<UpdateSettingsClusterStateUpdateRequest> {

    /**
     * Specifies the behaviour of an update-settings action on existing settings.
     */
    public enum OnExisting {
        /**
         * Update all the specified settings, overwriting any settings which already exist. This is the API default.
         */
        OVERWRITE,

        /**
         * Only add new settings, preserving the values of any settings which are already set and ignoring the new values specified in the
         * request.
         */
        PRESERVE
    }

    /**
     * Specifies the behaviour of an update-settings action which is trying to adjust a non-dynamic setting.
     */
    public enum OnStaticSetting {
        /**
         * Reject attempts to update non-dynamic settings on open indices. This is the API default.
         */
        REJECT,

        /**
         * Automatically close and reopen the shards of any open indices when updating a non-dynamic setting, forcing the shard to
         * reinitialize from scratch.
         */
        REOPEN_INDICES
    }

    private Settings settings;

    private boolean preserveExisting = false;

    private boolean reopenShards = false;

    public UpdateSettingsClusterStateUpdateRequest() {}

    @SuppressWarnings("this-escape")
    public UpdateSettingsClusterStateUpdateRequest(
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        Settings settings,
        OnExisting onExisting,
        OnStaticSetting onStaticSetting,
        Index... indices
    ) {
        masterNodeTimeout(masterNodeTimeout);
        ackTimeout(ackTimeout);
        settings(settings);
        setPreserveExisting(onExisting == OnExisting.PRESERVE);
        reopenShards(onStaticSetting == OnStaticSetting.REOPEN_INDICES);
        indices(indices);
    }

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
